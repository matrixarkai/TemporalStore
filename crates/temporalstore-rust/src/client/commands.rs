// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::Command;

use super::routing::key_is_dropped_by_percent;

pub(super) fn is_write(command: &Command) -> bool {
    // Delegate to the engine's canonical write-command classifier (the same one that gates WAL
    // persistence + dirty marking). The client uses this to refresh table topology before a write
    // (route to the current leader) and to pick the write retry budget, so it MUST agree with the
    // server on what mutates. A hand-maintained subset here had already drifted -- it omitted
    // seven context write commands the engine treats as writes (ContextWriteExtractedEvent,
    // ContextUpsertEntity/ChildRef/Embedding/Summary, ContextWriteCompressionEvent,
    // ContextCompressEvents) -- so issuing one of those through the public execute() API skipped
    // the pre-write topology refresh and got the read retry budget. Delegating keeps them in lockstep.
    crate::engine::is_write_command(command)
}

pub(super) fn command_is_dropped(command: &Command, drop_percent: u8) -> bool {
    // A full drain has to mean every command. The hash below can only judge commands that
    // HAVE a routing key, and the `unwrap_or(false)` on the end answers "keep sending it"
    // for the ones that do not -- the resource-blob upload path, sequence batch queries,
    // node-embedding reads. A table drained to 100 went on sending those.
    //
    // Below 100 that stands on purpose: shedding is per key by construction, so a command
    // with nothing to hash has no share to shed.
    if drop_percent >= 100 {
        return true;
    }
    command_routing_key(command)
        .as_deref()
        .map(|key| key_is_dropped_by_percent(key, drop_percent))
        .unwrap_or(false)
}

pub(super) fn command_key(command: &Command) -> Option<&str> {
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

pub(crate) fn command_routing_key(command: &Command) -> Option<String> {
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
        Command::ContextQuerySummaryVectors {
            tenant_hash,
            node_hashes,
            level,
            ..
        } => node_hashes
            .first()
            .map(|node_hash| context_summary_key(*tenant_hash, *node_hash, *level)),
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
            node_hash,
            ..
        } => Some(context_dirty_key(*tenant_hash, *node_hash)),
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_dirty_key(*tenant_hash, *node_hash)),
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_embedding_dirty_key(*tenant_hash, *node_hash)),
        Command::ContextQueryEmbeddingDirty {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_embedding_dirty_key(*tenant_hash, *node_hash)),
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
        // Keyed by the NODE: the vector lives on the node record.
        Command::ContextSetNodeEmbedding {
            tenant_hash,
            node_hash,
            ..
        } => Some(context_node_key(*tenant_hash, *node_hash)),
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

pub(super) fn context_embedding_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:embdirty:{tenant_hash}:{node_hash}")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_drain_drops_commands_that_carry_no_routing_key() {
        // The drop decision hashes a routing key and answered "not dropped" for every command
        // that has none -- the resource-blob upload path, sequence batch queries,
        // node-embedding reads. A table an operator had drained to 100 went on sending them.
        let keyless = Command::ContextResourceBlobPut {
            tenant_hash: 7,
            payload_base64: String::new(),
        };
        assert_eq!(
            command_routing_key(&keyless),
            None,
            "premise: this command carries no routing key to hash"
        );
        assert!(
            command_is_dropped(&keyless, 100),
            "a full drain has to drop a command with no key too, or the drain is not full"
        );

        // Below a full drain the leak stands on purpose: shedding is per key, and a command
        // with nothing to hash has no share to shed.
        assert!(!command_is_dropped(&keyless, 99));

        // A keyed command is unaffected in both directions.
        let keyed = Command::StringGet {
            key: "k".to_string(),
        };
        assert!(command_is_dropped(&keyed, 100));
        assert!(!command_is_dropped(&keyed, 0));
    }

    #[test]
    fn is_write_agrees_with_the_engine_for_context_writes() {
        // Regression: client is_write drives pre-write topology refresh + write retry budget and
        // must match the engine's is_write_command. A previously-omitted context write must now
        // classify as a write; a context read must still be a read.
        let context_write = Command::ContextCompressEvents {
            tenant_hash: 1,
            node_hash: 2,
            source_start_ms: 0,
            source_end_ms: 100,
            compressed_time_ms: 50,
            max_source_events: None,
            min_confidence: 0.0,
            min_importance: 0.0,
        };
        assert!(
            is_write(&context_write),
            "ContextCompressEvents mutates; the client must treat it as a write"
        );
        assert_eq!(
            is_write(&context_write),
            crate::engine::is_write_command(&context_write),
            "client is_write must stay in lockstep with the engine's is_write_command"
        );

        let context_read = Command::ContextGetNode {
            tenant_hash: 1,
            node_hash: 2,
        };
        assert!(
            !is_write(&context_read),
            "ContextGetNode is read-only; the client must not treat it as a write"
        );
    }
}
