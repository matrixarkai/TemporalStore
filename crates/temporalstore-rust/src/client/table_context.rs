// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreTable context methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn context_upsert_node(
        &self,
        tenant_hash: u64,
        node: ContextNode,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextUpsertNode { tenant_hash, node: Box::new(node) })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_upsert_node",
                response,
            }),
        }
    }

    pub fn context_get_node(
        &self,
        tenant_hash: u64,
        node_hash: u64,
    ) -> Result<Option<ContextNode>, ClientError> {
        match self
            .execute(Command::ContextGetNode {
                tenant_hash,
                node_hash,
            })?
            .response
        {
            CommandResponse::ContextNode { node, .. } => Ok(node),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_get_node",
                response,
            }),
        }
    }

    pub fn context_write_event(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        event: ContextEvent,
        first_write_only: bool,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextWriteEvent {
                tenant_hash,
                node_hash,
                event: Box::new(event),
                first_write_only,
                cold_storage: false,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_write_event",
                response,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn context_query_events(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
        current_valid_only: bool,
        as_of_ms: u64,
        kinds: Vec<u32>,
        statuses: Vec<u32>,
        min_confidence: f32,
        min_importance: f32,
    ) -> Result<Vec<ContextEvent>, ClientError> {
        match self
            .execute(Command::ContextQueryEvents {
                tenant_hash,
                node_hash,
                start_time_ms,
                end_time_ms,
                limit,
                max_scan: None,
                current_valid_only,
                as_of_ms,
                kinds,
                statuses,
                min_confidence,
                min_importance,
            })?
            .response
        {
            CommandResponse::ContextEvents { events, .. } => Ok(events),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_events",
                response,
            }),
        }
    }

    pub fn context_write_index_ref(
        &self,
        tenant_hash: u64,
        index_name: impl Into<String>,
        index_value_hash: u64,
        scope_hash: u64,
        event_time_ms: u64,
        index_ref: ContextIndexRef,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextWriteIndexRef {
                tenant_hash,
                index_name: index_name.into(),
                index_value_hash,
                scope_hash,
                event_time_ms,
                index_ref,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_write_index_ref",
                response,
            }),
        }
    }

    pub fn context_query_index(
        &self,
        tenant_hash: u64,
        index_name: impl Into<String>,
        index_value_hash: u64,
        scope_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
    ) -> Result<Vec<ContextIndexRef>, ClientError> {
        match self
            .execute(Command::ContextQueryIndex {
                tenant_hash,
                index_name: index_name.into(),
                index_value_hash,
                scope_hash,
                start_time_ms,
                end_time_ms,
                limit,
            })?
            .response
        {
            CommandResponse::ContextIndexRefs { refs, .. } => Ok(refs),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_index",
                response,
            }),
        }
    }

    pub fn context_write_pack_audit(
        &self,
        tenant_hash: u64,
        audit: ContextPackAudit,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextWritePackAudit { tenant_hash, audit })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_write_pack_audit",
                response,
            }),
        }
    }

    pub fn context_query_pack_audit(
        &self,
        tenant_hash: u64,
        session_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
    ) -> Result<Vec<ContextPackAudit>, ClientError> {
        match self
            .execute(Command::ContextQueryPackAudit {
                tenant_hash,
                session_hash,
                start_time_ms,
                end_time_ms,
                limit,
            })?
            .response
        {
            CommandResponse::ContextPackAudits { audits, .. } => Ok(audits),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_pack_audit",
                response,
            }),
        }
    }

    pub fn context_mark_summary_dirty(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        event_time_ms: u64,
        reason: u32,
        propagate_depth: u32,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextMarkSummaryDirty {
                tenant_hash,
                node_hash,
                event_time_ms,
                reason,
                propagate_depth,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_mark_summary_dirty",
                response,
            }),
        }
    }

    pub fn context_query_summary_dirty(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
    ) -> Result<Vec<ContextDirtyNode>, ClientError> {
        match self
            .execute(Command::ContextQuerySummaryDirty {
                tenant_hash,
                node_hash,
                start_time_ms,
                end_time_ms,
                limit,
            })?
            .response
        {
            CommandResponse::ContextSummaryDirtyNodes { nodes, .. } => Ok(nodes),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_summary_dirty",
                response,
            }),
        }
    }

    /// Mark a context node embedding-dirty (its semantic embedding is deferred or
    /// failed). Independent of the summary-dirty marker.
    pub fn context_mark_embedding_dirty(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        event_time_ms: u64,
        reason: u32,
        propagate_depth: u32,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextMarkEmbeddingDirty {
                tenant_hash,
                node_hash,
                event_time_ms,
                reason,
                propagate_depth,
                clear: false,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_mark_embedding_dirty",
                response,
            }),
        }
    }

    /// Clear the embedding-dirty marker for a node (called once it is embedded).
    pub fn context_clear_embedding_dirty(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        event_time_ms: u64,
        reason: u32,
        propagate_depth: u32,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextMarkEmbeddingDirty {
                tenant_hash,
                node_hash,
                event_time_ms,
                reason,
                propagate_depth,
                clear: true,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_clear_embedding_dirty",
                response,
            }),
        }
    }

    /// Query embedding-dirty nodes. `node_hash == 0` returns all pending
    /// embedding-dirty nodes (the drainer's O(pending) scan); a non-zero
    /// `node_hash` returns the single coalesced entry for that node. Returns the
    /// nodes alongside their per-marker tenant hashes (parallel vector; used by
    /// the all-pending drain scan, which spans tenants).
    pub fn context_query_embedding_dirty(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
    ) -> Result<(Vec<ContextDirtyNode>, Vec<u64>), ClientError> {
        match self
            .execute(Command::ContextQueryEmbeddingDirty {
                tenant_hash,
                node_hash,
                start_time_ms,
                end_time_ms,
                limit,
            })?
            .response
        {
            CommandResponse::ContextEmbeddingDirtyNodes {
                nodes,
                tenant_hashes,
                ..
            } => Ok((nodes, tenant_hashes)),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_embedding_dirty",
                response,
            }),
        }
    }
}
