// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::TemporalEngine;
use crate::llm_context::{
    EmbeddingChannel, EmbeddingHit, EmbeddingIndexError, EmbeddingKey, EmbeddingVector,
    EmbeddingWriteReport,
};
use crate::types::{Command, CONTEXT_SUMMARY_LEVEL_L1};

impl TemporalEngine {
    pub(super) fn command_updates_context_embedding(command: &Command) -> bool {
        match command {
            Command::ContextSetNodeEmbedding { .. } | Command::ContextUpsertNode { .. } => true,
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
                           item_id,
                           model_hash,
                           values: &[f32]| {
            let key = EmbeddingKey { tenant_id, item_id };
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
                    *node_hash,
                    *model_hash,
                    vector,
                ),
                Command::ContextUpsertNode { tenant_hash, node } => {
                    publish(
                        EmbeddingChannel::Node,
                        *tenant_hash,
                        node.node_hash,
                        node.embedding_model_hash,
                        &node.vector,
                    );
                    publish(
                        EmbeddingChannel::Summary,
                        *tenant_hash,
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
                    summary.node_hash,
                    summary.embedding_model_hash,
                    &summary.vector,
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

    pub(crate) fn score_context_embedding_candidates(
        &self,
        channel: EmbeddingChannel,
        tenant_hash: u64,
        model_hash: u64,
        query: &[f32],
        candidate_node_hashes: &[u64],
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        self.score_context_embedding_candidate_ids(
            channel,
            tenant_hash,
            model_hash,
            query,
            candidate_node_hashes.iter().copied(),
            candidate_node_hashes.len(),
        )
    }

    pub(crate) fn score_context_embedding_candidate_ids(
        &self,
        channel: EmbeddingChannel,
        tenant_hash: u64,
        model_hash: u64,
        query: &[f32],
        candidate_node_hashes: impl Clone + Iterator<Item = u64>,
        candidate_count: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .score_candidate_ids(
                channel,
                tenant_hash,
                model_hash,
                query,
                candidate_node_hashes,
                candidate_count,
            )
    }

    pub(crate) fn search_context_embeddings(
        &self,
        channel: EmbeddingChannel,
        tenant_hash: u64,
        model_hash: u64,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .search(channel, tenant_hash, model_hash, query, limit)
    }

    #[cfg(test)]
    pub(crate) fn context_embedding_index_len(&self) -> usize {
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}
