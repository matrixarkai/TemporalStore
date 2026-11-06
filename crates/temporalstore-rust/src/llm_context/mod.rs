// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Small Rust core for embedding-first LLM context ingestion and retrieval.
//!
//! This module intentionally contains no lexical scorer or metadata index. The
//! embedding index owns compact vectors and stable numeric keys; TemporalStore
//! records continue to own text, extraction output, and durable storage.

mod embedding_index;
mod ingest;
mod storage;

pub use embedding_index::{
    CompactEmbeddingBatch, EmbeddingHit, EmbeddingIndex, EmbeddingIndexConfig, EmbeddingIndexError,
    EmbeddingKey, EmbeddingVector, EmbeddingWriteReport,
};
pub use ingest::{
    BatchEmbedder, BatchExtractor, ContextEngineError, ContextIngestReport, ContextMessage,
    ContextRole, EmbeddingBatch, ExtractedContext, LlmContextEngine,
};
pub use storage::{EmbeddingRecoveryReport, TemporalEmbeddingStore, TemporalEmbeddingStoreError};
