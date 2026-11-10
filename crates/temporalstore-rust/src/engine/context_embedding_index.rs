// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::TemporalEngine;
use crate::llm_context::{
    EmbeddingChannel, EmbeddingHit, EmbeddingIndexError, EmbeddingKey, EmbeddingVector,
    EmbeddingWriteReport, PreparedEmbeddingQuery,
};
use crate::types::{Command, CONTEXT_SUMMARY_LEVEL_L1};

impl TemporalEngine {
    pub(super) fn command_updates_context_embedding(command: &Command) -> bool {
        match command {
            Command::ContextSetNodeEmbedding { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteExtractedEvent { .. }
            | Command::ContextUpsertEntity { .. } => true,
            Command::ContextUpsertSummary { summary, .. } => {
                summary.level == CONTEXT_SUMMARY_LEVEL_L1
            }
            _ => false,
        }
    }

    pub(super) fn publish_context_embedding_command(&self, command: &Command) {
        self.publish_context_embedding_commands(std::slice::from_ref(command));
    }

    pub(super) fn publish_context_embedding_commands(&self, commands: &[Command]) {
        if commands.is_empty() {
            return;
        }
        let mut registry = self
            .context_embedding_indexes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut publish = |channel: EmbeddingChannel,
                           tenant_id,
                           owner_id,
                           item_id,
                           model_hash,
                           values: &[f32]| {
            let key = EmbeddingKey {
                tenant_id,
                owner_id,
                item_id,
            };
            if values.is_empty() {
                registry.remove(channel, key);
            } else {
                let _ =
                    registry.upsert_batch(channel, model_hash, &[EmbeddingVector { key, values }]);
            }
        };
        for command in commands {
            match command {
                Command::ContextSetNodeEmbedding {
                    tenant_hash,
                    node_hash,
                    model_hash,
                    vector,
                    ..
                } => publish(
                    EmbeddingChannel::Node,
                    *tenant_hash,
                    0,
                    *node_hash,
                    *model_hash,
                    vector,
                ),
                Command::ContextUpsertNode { tenant_hash, node } => {
                    publish(
                        EmbeddingChannel::Node,
                        *tenant_hash,
                        0,
                        node.node_hash,
                        node.embedding_model_hash,
                        &node.vector,
                    );
                    publish(
                        EmbeddingChannel::Summary,
                        *tenant_hash,
                        0,
                        node.node_hash,
                        node.summary_vector_model_hash,
                        &node.summary_vector,
                    );
                }
                Command::ContextUpsertSummary {
                    tenant_hash,
                    summary,
                } if summary.level == CONTEXT_SUMMARY_LEVEL_L1 => publish(
                    EmbeddingChannel::Summary,
                    *tenant_hash,
                    0,
                    summary.node_hash,
                    summary.embedding_model_hash,
                    &summary.vector,
                ),
                Command::ContextWriteEvent {
                    tenant_hash,
                    node_hash,
                    event,
                    ..
                }
                | Command::ContextWriteExtractedEvent {
                    tenant_hash,
                    node_hash,
                    event,
                    ..
                } if event.event_id_hash != 0 => publish(
                    EmbeddingChannel::Event,
                    *tenant_hash,
                    *node_hash,
                    event.event_id_hash,
                    event.embedding_model_hash,
                    &event.vector,
                ),
                Command::ContextUpsertEntity {
                    tenant_hash,
                    entity,
                } => publish(
                    EmbeddingChannel::Entity,
                    *tenant_hash,
                    entity.node_hash,
                    entity.entity_hash,
                    entity.embedding_model_hash,
                    &entity.vector,
                ),
                _ => {}
            }
        }
    }

    pub(crate) fn cache_missing_context_embeddings(
        &self,
        channel: EmbeddingChannel,
        model_hash: u64,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<Option<EmbeddingWriteReport>, EmbeddingIndexError> {
        self.context_embedding_indexes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .upsert_missing_batch(channel, model_hash, batch)
    }

    pub(crate) fn visit_context_embedding_candidate_ids(
        &self,
        channel: EmbeddingChannel,
        tenant_hash: u64,
        model_hash: u64,
        query: PreparedEmbeddingQuery<'_>,
        candidate_node_hashes: impl Clone + Iterator<Item = u64>,
        visit: impl FnMut(EmbeddingHit),
    ) -> Result<usize, EmbeddingIndexError> {
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .visit_prepared_candidate_ids(
                channel,
                tenant_hash,
                model_hash,
                query,
                candidate_node_hashes,
                visit,
            )
    }

    pub(crate) fn search_context_embeddings(
        &self,
        channel: EmbeddingChannel,
        tenant_hash: u64,
        model_hash: u64,
        query: PreparedEmbeddingQuery<'_>,
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .search_prepared(channel, tenant_hash, model_hash, query, limit)
    }

    #[cfg(test)]
    pub(crate) fn context_embedding_index_len(&self) -> usize {
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}
