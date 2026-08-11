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
            .execute(Command::ContextUpsertNode { tenant_hash, node })?
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
                event,
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
        marker: ContextSummaryDirtyMarker,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextMarkSummaryDirty {
                tenant_hash,
                marker,
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
    ) -> Result<Vec<ContextSummaryDirtyMarker>, ClientError> {
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
            CommandResponse::ContextSummaryDirtyMarkers { markers, .. } => Ok(markers),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_query_summary_dirty",
                response,
            }),
        }
    }
}
