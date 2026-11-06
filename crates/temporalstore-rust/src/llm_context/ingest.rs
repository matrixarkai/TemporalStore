// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::error::Error;

use thiserror::Error;

use super::{
    EmbeddingHit, EmbeddingIndex, EmbeddingIndexConfig, EmbeddingIndexError, EmbeddingKey,
    EmbeddingVector,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextRole {
    User,
    Assistant,
    Tool,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextMessage {
    pub tenant_id: u64,
    pub message_id: u64,
    pub event_time_ms: u64,
    pub role: ContextRole,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedContext {
    pub tenant_id: u64,
    pub context_id: u64,
    pub source_message_id: u64,
    pub event_time_ms: u64,
    pub text: String,
}

/// Flat row-major embedding output avoids one allocation per extracted item.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingBatch {
    pub dimensions: usize,
    pub values: Vec<f32>,
}

impl EmbeddingBatch {
    pub fn rows(&self) -> usize {
        if self.dimensions == 0 {
            0
        } else {
            self.values.len() / self.dimensions
        }
    }

    fn row(&self, index: usize) -> &[f32] {
        let start = index * self.dimensions;
        &self.values[start..start + self.dimensions]
    }
}

pub trait BatchExtractor {
    type Error: Error + Send + Sync + 'static;

    fn extract_batch(
        &self,
        messages: &[ContextMessage],
    ) -> Result<Vec<ExtractedContext>, Self::Error>;
}

pub trait BatchEmbedder {
    type Error: Error + Send + Sync + 'static;

    fn embed_batch(&self, texts: &[&str]) -> Result<EmbeddingBatch, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextIngestReport {
    pub input_messages: usize,
    pub extracted_contexts: usize,
    pub inserted_embeddings: usize,
    pub updated_embeddings: usize,
    pub total_embeddings: usize,
    pub quantized_vector_bytes: usize,
}

#[derive(Debug, Error)]
pub enum ContextEngineError<X, E>
where
    X: Error + Send + Sync + 'static,
    E: Error + Send + Sync + 'static,
{
    #[error("context extraction failed")]
    Extraction(#[source] X),
    #[error("embedding generation failed")]
    Embedding(#[source] E),
    #[error("embedding output has {actual_rows} rows; expected {expected_rows}")]
    EmbeddingRowCount {
        expected_rows: usize,
        actual_rows: usize,
    },
    #[error("embedding output has {actual} dimensions; expected {expected}")]
    EmbeddingDimensions { expected: usize, actual: usize },
    #[error("embedding output length {values} is not divisible by {dimensions} dimensions")]
    EmbeddingShape { values: usize, dimensions: usize },
    #[error(transparent)]
    Index(#[from] EmbeddingIndexError),
}

pub struct LlmContextEngine<X, E>
where
    X: BatchExtractor,
    E: BatchEmbedder,
{
    extractor: X,
    embedder: E,
    index: EmbeddingIndex,
}

impl<X, E> LlmContextEngine<X, E>
where
    X: BatchExtractor,
    E: BatchEmbedder,
{
    pub fn new(
        config: EmbeddingIndexConfig,
        extractor: X,
        embedder: E,
    ) -> Result<Self, EmbeddingIndexError> {
        Ok(Self {
            extractor,
            embedder,
            index: EmbeddingIndex::new(config)?,
        })
    }

    pub fn index(&self) -> &EmbeddingIndex {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut EmbeddingIndex {
        &mut self.index
    }

    pub fn ingest_messages(
        &mut self,
        messages: &[ContextMessage],
    ) -> Result<ContextIngestReport, ContextEngineError<X::Error, E::Error>> {
        let extracted = self
            .extractor
            .extract_batch(messages)
            .map_err(ContextEngineError::Extraction)?;
        self.ingest_extracted(messages.len(), &extracted)
    }

    pub fn ingest_extracted(
        &mut self,
        input_messages: usize,
        extracted: &[ExtractedContext],
    ) -> Result<ContextIngestReport, ContextEngineError<X::Error, E::Error>> {
        if extracted.is_empty() {
            return Ok(ContextIngestReport {
                input_messages,
                extracted_contexts: 0,
                inserted_embeddings: 0,
                updated_embeddings: 0,
                total_embeddings: self.index.len(),
                quantized_vector_bytes: self.index.quantized_vector_bytes(),
            });
        }

        let texts: Vec<&str> = extracted.iter().map(|item| item.text.as_str()).collect();
        let embeddings = self
            .embedder
            .embed_batch(&texts)
            .map_err(ContextEngineError::Embedding)?;
        self.validate_embedding_batch(extracted.len(), &embeddings)?;

        let vectors: Vec<EmbeddingVector<'_>> = extracted
            .iter()
            .enumerate()
            .map(|(index, item)| EmbeddingVector {
                key: EmbeddingKey {
                    tenant_id: item.tenant_id,
                    item_id: item.context_id,
                },
                values: embeddings.row(index),
            })
            .collect();
        let write = self.index.upsert_batch(&vectors)?;
        Ok(ContextIngestReport {
            input_messages,
            extracted_contexts: extracted.len(),
            inserted_embeddings: write.inserted,
            updated_embeddings: write.updated,
            total_embeddings: write.total_entries,
            quantized_vector_bytes: write.quantized_vector_bytes,
        })
    }

    pub fn retrieve(
        &self,
        tenant_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, ContextEngineError<X::Error, E::Error>> {
        let embeddings = self
            .embedder
            .embed_batch(&[query])
            .map_err(ContextEngineError::Embedding)?;
        self.validate_embedding_batch(1, &embeddings)?;
        Ok(self.index.search(tenant_id, embeddings.row(0), limit)?)
    }

    fn validate_embedding_batch(
        &self,
        expected_rows: usize,
        batch: &EmbeddingBatch,
    ) -> Result<(), ContextEngineError<X::Error, E::Error>> {
        if batch.dimensions == 0 || batch.values.len() % batch.dimensions != 0 {
            return Err(ContextEngineError::EmbeddingShape {
                values: batch.values.len(),
                dimensions: batch.dimensions,
            });
        }
        if batch.dimensions != self.index.dimensions() {
            return Err(ContextEngineError::EmbeddingDimensions {
                expected: self.index.dimensions(),
                actual: batch.dimensions,
            });
        }
        let actual_rows = batch.rows();
        if actual_rows != expected_rows {
            return Err(ContextEngineError::EmbeddingRowCount {
                expected_rows,
                actual_rows,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    struct ExtractEveryMessage;

    impl BatchExtractor for ExtractEveryMessage {
        type Error = Infallible;

        fn extract_batch(
            &self,
            messages: &[ContextMessage],
        ) -> Result<Vec<ExtractedContext>, Self::Error> {
            Ok(messages
                .iter()
                .map(|message| ExtractedContext {
                    tenant_id: message.tenant_id,
                    context_id: message.message_id,
                    source_message_id: message.message_id,
                    event_time_ms: message.event_time_ms,
                    text: message.text.clone(),
                })
                .collect())
        }
    }

    struct TwoDimensionalEmbedder;

    impl BatchEmbedder for TwoDimensionalEmbedder {
        type Error = Infallible;

        fn embed_batch(&self, texts: &[&str]) -> Result<EmbeddingBatch, Self::Error> {
            let mut values = Vec::with_capacity(texts.len() * 2);
            for text in texts {
                if text.contains("database") {
                    values.extend_from_slice(&[1.0, 0.0]);
                } else {
                    values.extend_from_slice(&[0.0, 1.0]);
                }
            }
            Ok(EmbeddingBatch {
                dimensions: 2,
                values,
            })
        }
    }

    #[test]
    fn one_batched_pipeline_ingests_and_retrieves_context() {
        let mut engine = LlmContextEngine::new(
            EmbeddingIndexConfig::new(2, 16),
            ExtractEveryMessage,
            TwoDimensionalEmbedder,
        )
        .unwrap();
        let messages = vec![
            ContextMessage {
                tenant_id: 5,
                message_id: 20,
                event_time_ms: 1,
                role: ContextRole::User,
                text: "database recovery design".to_string(),
            },
            ContextMessage {
                tenant_id: 5,
                message_id: 21,
                event_time_ms: 2,
                role: ContextRole::Assistant,
                text: "response formatting".to_string(),
            },
        ];
        let report = engine.ingest_messages(&messages).unwrap();
        assert_eq!(report.inserted_embeddings, 2);
        let hits = engine.retrieve(5, "database durability", 1).unwrap();
        assert_eq!(hits[0].key.item_id, 20);
    }
}
