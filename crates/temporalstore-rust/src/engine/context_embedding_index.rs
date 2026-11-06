// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::TemporalEngine;
use crate::llm_context::{
    EmbeddingChannel, EmbeddingHit, EmbeddingIndexError, EmbeddingVector, EmbeddingWriteReport,
};

impl TemporalEngine {
    pub(crate) fn cache_context_embeddings(
        &self,
        channel: EmbeddingChannel,
        model_hash: u64,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<Option<EmbeddingWriteReport>, EmbeddingIndexError> {
        self.context_embedding_indexes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .upsert_batch(channel, model_hash, batch)
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
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .score_candidates(
                channel,
                tenant_hash,
                model_hash,
                query,
                candidate_node_hashes,
            )
    }

    #[cfg(test)]
    pub(crate) fn context_embedding_index_len(&self) -> usize {
        self.context_embedding_indexes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}
