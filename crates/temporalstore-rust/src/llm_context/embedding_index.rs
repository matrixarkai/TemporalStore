// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};

use thiserror::Error;

const MAX_EMBEDDING_DIMENSIONS: usize = 4096;
const SIGNATURE_ROUTING_MIN_DIMENSIONS: usize = 384;
const SIGNATURE_ROUTING_MIN_ROWS: usize = 8192;
const SIGNATURE_ROUTING_TABLES: usize = 4;
const SIGNATURE_ROUTING_BITS: usize = 12;
const SIGNATURE_ROUTING_MASK: u16 = (1 << SIGNATURE_ROUTING_BITS) - 1;
const SIGNATURE_SAMPLES_PER_BIT: usize = 16;
const SIGNATURE_CANDIDATES_PER_HIT: usize = 128;
const SIGNATURE_MIN_CANDIDATES: usize = 4096;
const SIGNATURE_MAX_CANDIDATES: usize = 32_768;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EmbeddingKey {
    pub tenant_id: u64,
    pub item_id: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct EmbeddingVector<'a> {
    pub key: EmbeddingKey,
    pub values: &'a [f32],
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmbeddingHit {
    pub key: EmbeddingKey,
    pub score: f32,
}

/// Exact cosine scores for a bounded rerank set.
///
/// The compact index should select the set. This function is deliberately
/// allocation-light and does not own or retain application vectors.
pub fn score_embedding_candidates_exact(
    query: &[f32],
    candidates: &[EmbeddingVector<'_>],
) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
    let query_inverse_norm = query_inverse_norm(query, query.len())?;
    let mut hits = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if candidate.values.len() != query.len() {
            return Err(EmbeddingIndexError::DimensionMismatch {
                key: candidate.key,
                expected: query.len(),
                actual: candidate.values.len(),
            });
        }
        let mut dot = 0.0f32;
        let mut norm_squared = 0.0f32;
        for (dimension, value) in candidate.values.iter().enumerate() {
            if !value.is_finite() {
                return Err(EmbeddingIndexError::NonFiniteValue {
                    key: candidate.key,
                    dimension,
                });
            }
            dot += query[dimension] * value;
            norm_squared += value * value;
        }
        if norm_squared <= f32::EPSILON {
            return Err(EmbeddingIndexError::ZeroNorm { key: candidate.key });
        }
        hits.push(EmbeddingHit {
            key: candidate.key,
            score: dot * query_inverse_norm * norm_squared.sqrt().recip(),
        });
    }
    Ok(hits)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingIndexConfig {
    pub dimensions: usize,
    pub max_entries: usize,
    pub model_hash: u64,
}

impl EmbeddingIndexConfig {
    pub fn new(dimensions: usize, max_entries: usize) -> Self {
        Self {
            dimensions,
            max_entries,
            model_hash: 0,
        }
    }

    pub fn with_model_hash(mut self, model_hash: u64) -> Self {
        self.model_hash = model_hash;
        self
    }
}

/// Quantized rows prepared once for durable storage and hot-index publication.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactEmbeddingBatch {
    dimensions: usize,
    model_hash: u64,
    keys: Vec<EmbeddingKey>,
    scales: Vec<f32>,
    vectors: Vec<i8>,
}

impl CompactEmbeddingBatch {
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn model_hash(&self) -> u64 {
        self.model_hash
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn key(&self, row: usize) -> EmbeddingKey {
        self.keys[row]
    }

    pub fn scale(&self, row: usize) -> f32 {
        self.scales[row]
    }

    pub fn vector(&self, row: usize) -> &[i8] {
        let start = row * self.dimensions;
        &self.vectors[start..start + self.dimensions]
    }

    pub(crate) fn from_compact_rows(
        dimensions: usize,
        model_hash: u64,
        keys: Vec<EmbeddingKey>,
        scales: Vec<f32>,
        vectors: Vec<i8>,
    ) -> Result<Self, EmbeddingIndexError> {
        if dimensions == 0 || dimensions > MAX_EMBEDDING_DIMENSIONS {
            return Err(EmbeddingIndexError::InvalidDimensions);
        }
        if keys.len() != scales.len() || vectors.len() != keys.len().saturating_mul(dimensions) {
            return Err(EmbeddingIndexError::CompactBatchShape);
        }
        let mut unique = HashSet::with_capacity(keys.len());
        for (row, key) in keys.iter().copied().enumerate() {
            if !unique.insert(key) {
                return Err(EmbeddingIndexError::DuplicateKey { key });
            }
            let scale = scales[row];
            if !scale.is_finite() || scale <= 0.0 {
                return Err(EmbeddingIndexError::InvalidCompactScale { key });
            }
        }
        Ok(Self {
            dimensions,
            model_hash,
            keys,
            scales,
            vectors,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingWriteReport {
    pub inserted: usize,
    pub updated: usize,
    pub total_entries: usize,
    pub quantized_vector_bytes: usize,
}

#[derive(Debug, Error, PartialEq)]
pub enum EmbeddingIndexError {
    #[error("embedding dimensions must be between 1 and {MAX_EMBEDDING_DIMENSIONS}")]
    InvalidDimensions,
    #[error("embedding index capacity must be greater than zero")]
    InvalidCapacity,
    #[error("embedding index capacity {capacity} would be exceeded by {requested} entries")]
    CapacityExceeded { capacity: usize, requested: usize },
    #[error("embedding vector for {key:?} has {actual} dimensions; expected {expected}")]
    DimensionMismatch {
        key: EmbeddingKey,
        expected: usize,
        actual: usize,
    },
    #[error("embedding vector for {key:?} contains a non-finite value at dimension {dimension}")]
    NonFiniteValue { key: EmbeddingKey, dimension: usize },
    #[error("embedding vector for {key:?} has zero norm")]
    ZeroNorm { key: EmbeddingKey },
    #[error("embedding batch contains key {key:?} more than once")]
    DuplicateKey { key: EmbeddingKey },
    #[error("compact embedding batch has inconsistent row storage")]
    CompactBatchShape,
    #[error("compact embedding row for {key:?} has an invalid scale")]
    InvalidCompactScale { key: EmbeddingKey },
    #[error("embedding model hash {actual} does not match index model hash {expected}")]
    ModelMismatch { expected: u64, actual: u64 },
    #[error("query vector has {actual} dimensions; expected {expected}")]
    QueryDimensionMismatch { expected: usize, actual: usize },
    #[error("query vector contains a non-finite value at dimension {dimension}")]
    QueryNonFiniteValue { dimension: usize },
    #[error("query vector has zero norm")]
    QueryZeroNorm,
}

/// Exact top-k scan over compact, row-major vectors.
///
/// Each stored row uses one byte per dimension plus a four-byte scale. Text and
/// application metadata are deliberately not duplicated here.
#[derive(Debug)]
pub struct EmbeddingIndex {
    config: EmbeddingIndexConfig,
    tenants: HashMap<u64, TenantRows>,
    total_entries: usize,
}

#[derive(Debug, Default)]
struct TenantRows {
    item_ids: Vec<u64>,
    positions: HashMap<u64, usize>,
    scales: Vec<f32>,
    vectors: Vec<i8>,
    signature_routing: Option<SignatureRouting>,
}

#[derive(Debug)]
struct SignatureRouting {
    // Buckets retain compact row positions only; vectors stay in TenantRows.
    buckets: [HashMap<u16, Vec<u32>>; SIGNATURE_ROUTING_TABLES],
}

enum SeenPositions {
    Dense(Vec<u64>),
    Sparse(HashSet<u32>),
}

impl SeenPositions {
    fn new(row_count: usize, budget: usize) -> Self {
        let dense_words = row_count.div_ceil(u64::BITS as usize);
        let dense_bytes = dense_words.saturating_mul(std::mem::size_of::<u64>());
        let sparse_key_bytes = budget.saturating_mul(std::mem::size_of::<u32>());
        // Never trade more query memory for direct bit tests.
        if dense_bytes <= sparse_key_bytes {
            Self::Dense(vec![0; dense_words])
        } else {
            Self::Sparse(HashSet::with_capacity(budget))
        }
    }

    fn insert(&mut self, position: u32) -> bool {
        match self {
            Self::Dense(words) => {
                let position = position as usize;
                let word = position / u64::BITS as usize;
                let mask = 1u64 << (position % u64::BITS as usize);
                let inserted = words[word] & mask == 0;
                words[word] |= mask;
                inserted
            }
            Self::Sparse(positions) => positions.insert(position),
        }
    }
}

impl SignatureRouting {
    fn build(rows: &TenantRows, dimensions: usize) -> Self {
        let mut routing = Self {
            buckets: std::array::from_fn(|_| HashMap::new()),
        };
        for position in 0..rows.item_ids.len() {
            let start = position * dimensions;
            let signature = embedding_signature_i8(&rows.vectors[start..start + dimensions]);
            routing.insert(signature, position);
        }
        routing
    }

    fn insert(&mut self, signature: u64, position: usize) {
        let Ok(position) = u32::try_from(position) else {
            return;
        };
        for table in 0..SIGNATURE_ROUTING_TABLES {
            self.buckets[table]
                .entry(signature_bucket(signature, table))
                .or_default()
                .push(position);
        }
    }

    fn remove(&mut self, signature: u64, position: usize) {
        let Ok(position) = u32::try_from(position) else {
            return;
        };
        for table in 0..SIGNATURE_ROUTING_TABLES {
            let bucket_key = signature_bucket(signature, table);
            let empty = if let Some(bucket) = self.buckets[table].get_mut(&bucket_key) {
                if let Some(offset) = bucket.iter().position(|candidate| *candidate == position) {
                    bucket.swap_remove(offset);
                }
                bucket.is_empty()
            } else {
                false
            };
            if empty {
                self.buckets[table].remove(&bucket_key);
            }
        }
    }

    fn candidates(&self, signature: u64, row_count: usize, budget: usize) -> Vec<u32> {
        // Routing stores u32 positions, so keep query scratch at that width until scoring.
        let mut positions = Vec::with_capacity(budget);
        let mut seen = SeenPositions::new(row_count, budget);
        for distance in 0..=2 {
            for table in 0..SIGNATURE_ROUTING_TABLES {
                let bucket = signature_bucket(signature, table);
                match distance {
                    0 => {
                        if self.extend_bucket(table, bucket, budget, &mut seen, &mut positions) {
                            return positions;
                        }
                    }
                    1 => {
                        for first in 0..SIGNATURE_ROUTING_BITS {
                            if self.extend_bucket(
                                table,
                                bucket ^ (1 << first),
                                budget,
                                &mut seen,
                                &mut positions,
                            ) {
                                return positions;
                            }
                        }
                    }
                    2 => {
                        for first in 0..SIGNATURE_ROUTING_BITS {
                            for second in first + 1..SIGNATURE_ROUTING_BITS {
                                if self.extend_bucket(
                                    table,
                                    bucket ^ (1 << first) ^ (1 << second),
                                    budget,
                                    &mut seen,
                                    &mut positions,
                                ) {
                                    return positions;
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        positions
    }

    fn extend_bucket(
        &self,
        table: usize,
        bucket: u16,
        budget: usize,
        seen: &mut SeenPositions,
        positions: &mut Vec<u32>,
    ) -> bool {
        let Some(bucket_positions) = self.buckets[table].get(&bucket) else {
            return false;
        };
        for position in bucket_positions {
            if seen.insert(*position) {
                positions.push(*position);
                if positions.len() >= budget {
                    return true;
                }
            }
        }
        false
    }
}

impl TenantRows {
    fn signature_at(&self, dimensions: usize, position: usize) -> Option<u64> {
        self.signature_routing.as_ref()?;
        let start = position * dimensions;
        Some(embedding_signature_i8(
            &self.vectors[start..start + dimensions],
        ))
    }

    fn refresh_signature_routing(
        &mut self,
        dimensions: usize,
        position: usize,
        old_signature: Option<u64>,
    ) {
        if position > u32::MAX as usize {
            self.signature_routing = None;
            return;
        }
        if self.signature_routing.is_some() {
            let start = position * dimensions;
            let new_signature =
                embedding_signature_i8(&self.vectors[start..start + dimensions]);
            let routing = self
                .signature_routing
                .as_mut()
                .expect("signature routing exists");
            if let Some(old_signature) = old_signature {
                routing.remove(old_signature, position);
            }
            routing.insert(new_signature, position);
        } else if dimensions >= SIGNATURE_ROUTING_MIN_DIMENSIONS
            && self.item_ids.len() >= SIGNATURE_ROUTING_MIN_ROWS
            && self.item_ids.len() <= u32::MAX as usize
        {
            self.signature_routing = Some(SignatureRouting::build(self, dimensions));
        }
    }
}

impl EmbeddingIndex {
    pub fn new(config: EmbeddingIndexConfig) -> Result<Self, EmbeddingIndexError> {
        if config.dimensions == 0 || config.dimensions > MAX_EMBEDDING_DIMENSIONS {
            return Err(EmbeddingIndexError::InvalidDimensions);
        }
        if config.max_entries == 0 {
            return Err(EmbeddingIndexError::InvalidCapacity);
        }
        Ok(Self {
            config,
            tenants: HashMap::new(),
            total_entries: 0,
        })
    }

    pub fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    pub fn model_hash(&self) -> u64 {
        self.config.model_hash
    }

    pub fn len(&self) -> usize {
        self.total_entries
    }

    pub fn is_empty(&self) -> bool {
        self.total_entries == 0
    }

    pub fn quantized_vector_bytes(&self) -> usize {
        self.total_entries
            .saturating_mul(self.config.dimensions + std::mem::size_of::<f32>())
    }

    pub fn contains(&self, key: EmbeddingKey) -> bool {
        self.tenants
            .get(&key.tenant_id)
            .is_some_and(|rows| rows.positions.contains_key(&key.item_id))
    }

    pub fn upsert_batch(
        &mut self,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<EmbeddingWriteReport, EmbeddingIndexError> {
        if let [input] = batch {
            return self.upsert_one(*input);
        }
        let compact = self.prepare_batch(batch)?;
        self.upsert_compact_batch(&compact)
    }

    fn upsert_one(
        &mut self,
        input: EmbeddingVector<'_>,
    ) -> Result<EmbeddingWriteReport, EmbeddingIndexError> {
        self.validate_vector_dimensions(input.key, input.values)?;
        let exists = self.contains(input.key);
        let requested = self.len().saturating_add(usize::from(!exists));
        if requested > self.config.max_entries {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: self.config.max_entries,
                requested,
            });
        }
        let (scale, quantization_multiplier) = quantization_parameters(input.key, input.values)?;

        let rows = self.tenants.entry(input.key.tenant_id).or_default();
        let (inserted, updated) = if let Some(&position) = rows.positions.get(&input.key.item_id) {
            let old_signature = rows.signature_at(self.config.dimensions, position);
            rows.scales[position] = scale;
            let start = position * self.config.dimensions;
            quantize_into(
                input.values,
                quantization_multiplier,
                &mut rows.vectors[start..start + self.config.dimensions],
            );
            rows.refresh_signature_routing(self.config.dimensions, position, old_signature);
            (0, 1)
        } else {
            let position = rows.item_ids.len();
            rows.item_ids.push(input.key.item_id);
            rows.positions.insert(input.key.item_id, position);
            rows.scales.push(scale);
            rows.vectors.extend(
                input
                    .values
                    .iter()
                    .map(|value| quantize_value(*value, quantization_multiplier)),
            );
            rows.refresh_signature_routing(self.config.dimensions, position, None);
            self.total_entries += 1;
            (1, 0)
        };

        Ok(EmbeddingWriteReport {
            inserted,
            updated,
            total_entries: self.len(),
            quantized_vector_bytes: self.quantized_vector_bytes(),
        })
    }

    pub fn prepare_batch(
        &self,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<CompactEmbeddingBatch, EmbeddingIndexError> {
        let compact = self.prepare_batch_unbounded(batch)?;
        let new_entries = (0..compact.len())
            .filter(|row| !self.contains(compact.key(*row)))
            .count();
        let requested = self.len().saturating_add(new_entries);
        if requested > self.config.max_entries {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: self.config.max_entries,
                requested,
            });
        }
        Ok(compact)
    }

    pub(crate) fn prepare_batch_unbounded(
        &self,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<CompactEmbeddingBatch, EmbeddingIndexError> {
        let mut batch_keys = HashSet::with_capacity(batch.len());
        for input in batch {
            self.validate_vector_dimensions(input.key, input.values)?;
            if !batch_keys.insert(input.key) {
                return Err(EmbeddingIndexError::DuplicateKey { key: input.key });
            }
        }

        let mut keys = Vec::with_capacity(batch.len());
        let mut scales = Vec::with_capacity(batch.len());
        let mut vectors = Vec::with_capacity(batch.len().saturating_mul(self.config.dimensions));
        for input in batch {
            keys.push(input.key);
            scales.push(quantize_unit_vector(input.key, input.values, &mut vectors)?);
        }
        CompactEmbeddingBatch::from_compact_rows(
            self.config.dimensions,
            self.config.model_hash,
            keys,
            scales,
            vectors,
        )
    }

    pub fn upsert_compact_batch(
        &mut self,
        batch: &CompactEmbeddingBatch,
    ) -> Result<EmbeddingWriteReport, EmbeddingIndexError> {
        if batch.dimensions != self.config.dimensions {
            return Err(EmbeddingIndexError::DimensionMismatch {
                key: batch.keys.first().copied().unwrap_or(EmbeddingKey {
                    tenant_id: 0,
                    item_id: 0,
                }),
                expected: self.config.dimensions,
                actual: batch.dimensions,
            });
        }
        if batch.model_hash != self.config.model_hash {
            return Err(EmbeddingIndexError::ModelMismatch {
                expected: self.config.model_hash,
                actual: batch.model_hash,
            });
        }
        let new_entries = batch
            .keys
            .iter()
            .filter(|key| {
                !self
                    .tenants
                    .get(&key.tenant_id)
                    .is_some_and(|rows| rows.positions.contains_key(&key.item_id))
            })
            .count();
        let requested = self.len().saturating_add(new_entries);
        if requested > self.config.max_entries {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: self.config.max_entries,
                requested,
            });
        }

        let mut inserted = 0usize;
        let mut updated = 0usize;
        for row in 0..batch.len() {
            let key = batch.key(row);
            let scale = batch.scale(row);
            let vector = batch.vector(row);
            let rows = self.tenants.entry(key.tenant_id).or_default();
            if let Some(&position) = rows.positions.get(&key.item_id) {
                let old_signature = rows.signature_at(self.config.dimensions, position);
                rows.scales[position] = scale;
                let start = position * self.config.dimensions;
                rows.vectors[start..start + self.config.dimensions].copy_from_slice(vector);
                rows.refresh_signature_routing(
                    self.config.dimensions,
                    position,
                    old_signature,
                );
                updated += 1;
            } else {
                let position = rows.item_ids.len();
                rows.item_ids.push(key.item_id);
                rows.positions.insert(key.item_id, position);
                rows.scales.push(scale);
                rows.vectors.extend_from_slice(vector);
                rows.refresh_signature_routing(self.config.dimensions, position, None);
                inserted += 1;
            }
        }
        self.total_entries += inserted;

        Ok(EmbeddingWriteReport {
            inserted,
            updated,
            total_entries: self.len(),
            quantized_vector_bytes: self.quantized_vector_bytes(),
        })
    }

    /// Inserts a prepared cache batch, recycling unprotected rows when the index is full.
    ///
    /// Durable vectors are owned by the caller; eviction only changes this compact hot index.
    pub(crate) fn upsert_compact_batch_bounded(
        &mut self,
        batch: &CompactEmbeddingBatch,
    ) -> Result<EmbeddingWriteReport, EmbeddingIndexError> {
        if batch.len() > self.config.max_entries {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: self.config.max_entries,
                requested: batch.len(),
            });
        }
        let protected = (0..batch.len())
            .map(|row| batch.key(row))
            .collect::<HashSet<_>>();
        let new_entries = protected.iter().filter(|key| !self.contains(**key)).count();
        let evict = self
            .len()
            .saturating_add(new_entries)
            .saturating_sub(self.config.max_entries);
        if evict > 0 {
            let victims = self
                .tenants
                .iter()
                .flat_map(|(tenant_id, rows)| {
                    rows.item_ids.iter().map(move |item_id| EmbeddingKey {
                        tenant_id: *tenant_id,
                        item_id: *item_id,
                    })
                })
                .filter(|key| !protected.contains(key))
                .take(evict)
                .collect::<Vec<_>>();
            if victims.len() != evict {
                return Err(EmbeddingIndexError::CapacityExceeded {
                    capacity: self.config.max_entries,
                    requested: self.len().saturating_add(new_entries),
                });
            }
            for key in victims {
                self.remove(key);
            }
        }
        self.upsert_compact_batch(batch)
    }

    pub fn remove(&mut self, key: EmbeddingKey) -> bool {
        let Some(rows) = self.tenants.get_mut(&key.tenant_id) else {
            return false;
        };
        let Some(position) = rows.positions.remove(&key.item_id) else {
            return false;
        };
        let last = rows.item_ids.len() - 1;
        let routing_move = rows.signature_routing.as_ref().map(|_| {
            let removed_start = position * self.config.dimensions;
            let removed = embedding_signature_i8(
                &rows.vectors[removed_start..removed_start + self.config.dimensions],
            );
            let moved = if position != last {
                let moved_start = last * self.config.dimensions;
                Some(embedding_signature_i8(
                    &rows.vectors[moved_start..moved_start + self.config.dimensions],
                ))
            } else {
                None
            };
            (removed, moved)
        });
        if let Some((removed, moved)) = routing_move {
            let routing = rows
                .signature_routing
                .as_mut()
                .expect("signature routing exists");
            routing.remove(removed, position);
            if let Some(moved) = moved {
                routing.remove(moved, last);
                routing.insert(moved, position);
            }
        }
        if position != last {
            let source = last * self.config.dimensions;
            let target = position * self.config.dimensions;
            rows.vectors
                .copy_within(source..source + self.config.dimensions, target);
        }
        rows.vectors
            .truncate(last.saturating_mul(self.config.dimensions));
        rows.item_ids.swap_remove(position);
        rows.scales.swap_remove(position);
        if position != last {
            rows.positions.insert(rows.item_ids[position], position);
        }
        if rows.item_ids.len() < SIGNATURE_ROUTING_MIN_ROWS {
            rows.signature_routing = None;
        }
        self.total_entries -= 1;
        if rows.item_ids.is_empty() {
            self.tenants.remove(&key.tenant_id);
        }
        true
    }

    pub fn search(
        &self,
        tenant_id: u64,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(rows) = self.tenants.get(&tenant_id) else {
            return Ok(Vec::new());
        };
        let query_inverse_norm = query_inverse_norm(query, self.config.dimensions)?;
        let limit = limit.min(rows.item_ids.len());
        // Large tenants route first, then reuse the exact compact scorer on a bounded set.
        if let Some(routing) = &rows.signature_routing {
            let candidate_budget = limit
                .saturating_mul(SIGNATURE_CANDIDATES_PER_HIT)
                .clamp(SIGNATURE_MIN_CANDIDATES, SIGNATURE_MAX_CANDIDATES)
                .min(rows.item_ids.len());
            let positions = routing.candidates(
                embedding_signature_f32(query),
                rows.item_ids.len(),
                candidate_budget,
            );
            let minimum_routed_candidates = limit.saturating_mul(4).min(rows.item_ids.len());
            if positions.len() >= minimum_routed_candidates {
                return Ok(self.search_positions(
                    tenant_id,
                    rows,
                    query,
                    query_inverse_norm,
                    limit,
                    positions.into_iter().map(|position| position as usize),
                ));
            }
        }
        Ok(self.search_positions(
            tenant_id,
            rows,
            query,
            query_inverse_norm,
            limit,
            0..rows.item_ids.len(),
        ))
    }

    fn search_positions(
        &self,
        tenant_id: u64,
        rows: &TenantRows,
        query: &[f32],
        query_inverse_norm: f32,
        limit: usize,
        positions: impl IntoIterator<Item = usize>,
    ) -> Vec<EmbeddingHit> {
        let mut heap = BinaryHeap::<Reverse<HeapHit>>::with_capacity(limit);

        for position in positions {
            let start = position * self.config.dimensions;
            let row = &rows.vectors[start..start + self.config.dimensions];
            let score = (dot_i8_f32(row, query) * rows.scales[position] * query_inverse_norm)
                .clamp(-1.0, 1.0);
            let candidate = HeapHit { score, position };
            if heap.len() < limit {
                heap.push(Reverse(candidate));
            } else if heap.peek().is_some_and(|smallest| candidate > smallest.0) {
                heap.pop();
                heap.push(Reverse(candidate));
            }
        }

        let mut hits: Vec<EmbeddingHit> = heap
            .into_iter()
            .map(|Reverse(hit)| EmbeddingHit {
                key: EmbeddingKey {
                    tenant_id,
                    item_id: rows.item_ids[hit.position],
                },
                score: hit.score,
            })
            .collect();
        hits.sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.key.item_id.cmp(&right.key.item_id))
        });
        hits
    }

    /// Score only the requested item ids without scanning the rest of the tenant.
    ///
    /// Results preserve candidate order so callers that already own a ranking
    /// pipeline can merge scores without paying for an extra sort.
    pub fn score_candidates(
        &self,
        tenant_id: u64,
        query: &[f32],
        candidate_item_ids: &[u64],
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        self.score_candidate_ids(
            tenant_id,
            query,
            candidate_item_ids.iter().copied(),
            candidate_item_ids.len(),
        )
    }

    pub(crate) fn score_candidate_ids(
        &self,
        tenant_id: u64,
        query: &[f32],
        candidate_item_ids: impl IntoIterator<Item = u64>,
        candidate_count: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        let Some(rows) = self.tenants.get(&tenant_id) else {
            return Ok(Vec::new());
        };
        let query_inverse_norm = query_inverse_norm(query, self.config.dimensions)?;
        let mut hits = Vec::with_capacity(candidate_count.min(rows.item_ids.len()));
        for item_id in candidate_item_ids {
            let Some(&position) = rows.positions.get(&item_id) else {
                continue;
            };
            let start = position * self.config.dimensions;
            let row = &rows.vectors[start..start + self.config.dimensions];
            hits.push(EmbeddingHit {
                key: EmbeddingKey {
                    tenant_id,
                    item_id,
                },
                score: (dot_i8_f32(row, query) * rows.scales[position] * query_inverse_norm)
                    .clamp(-1.0, 1.0),
            });
        }
        Ok(hits)
    }

    fn validate_vector_dimensions(
        &self,
        key: EmbeddingKey,
        values: &[f32],
    ) -> Result<(), EmbeddingIndexError> {
        if values.len() != self.config.dimensions {
            return Err(EmbeddingIndexError::DimensionMismatch {
                key,
                expected: self.config.dimensions,
                actual: values.len(),
            });
        }
        Ok(())
    }
}

fn query_inverse_norm(
    query: &[f32],
    expected_dimensions: usize,
) -> Result<f32, EmbeddingIndexError> {
    if query.len() != expected_dimensions {
        return Err(EmbeddingIndexError::QueryDimensionMismatch {
            expected: expected_dimensions,
            actual: query.len(),
        });
    }
    let mut norm_squared = 0.0f64;
    for (dimension, value) in query.iter().enumerate() {
        if !value.is_finite() {
            return Err(EmbeddingIndexError::QueryNonFiniteValue { dimension });
        }
        norm_squared += f64::from(*value) * f64::from(*value);
    }
    if norm_squared <= f64::EPSILON {
        return Err(EmbeddingIndexError::QueryZeroNorm);
    }
    Ok(norm_squared.sqrt().recip() as f32)
}

#[derive(Debug, Clone, Copy)]
struct HeapHit {
    score: f32,
    position: usize,
}

impl PartialEq for HeapHit {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.position == other.position
    }
}

impl Eq for HeapHit {}

impl PartialOrd for HeapHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.position.cmp(&other.position))
    }
}

fn quantize_unit_vector(
    key: EmbeddingKey,
    values: &[f32],
    output: &mut Vec<i8>,
) -> Result<f32, EmbeddingIndexError> {
    let (scale, quantization_multiplier) = quantization_parameters(key, values)?;
    output.extend(
        values
            .iter()
            .map(|value| quantize_value(*value, quantization_multiplier)),
    );
    Ok(scale)
}

fn quantization_parameters(
    key: EmbeddingKey,
    values: &[f32],
) -> Result<(f32, f32), EmbeddingIndexError> {
    let mut norm_squared = 0.0f64;
    let mut peak = 0.0f32;
    for (dimension, value) in values.iter().copied().enumerate() {
        if !value.is_finite() {
            return Err(EmbeddingIndexError::NonFiniteValue { key, dimension });
        }
        norm_squared += f64::from(value) * f64::from(value);
        peak = peak.max(value.abs());
    }
    if norm_squared <= f64::EPSILON {
        return Err(EmbeddingIndexError::ZeroNorm { key });
    }

    let scale = peak * (norm_squared.sqrt().recip() as f32) / 127.0;
    let quantization_multiplier = 127.0 / peak;
    Ok((scale, quantization_multiplier))
}

fn quantize_into(values: &[f32], quantization_multiplier: f32, output: &mut [i8]) {
    for (slot, value) in output.iter_mut().zip(values) {
        *slot = quantize_value(*value, quantization_multiplier);
    }
}

#[inline]
fn quantize_value(value: f32, quantization_multiplier: f32) -> i8 {
    (value * quantization_multiplier)
        .round()
        .clamp(-127.0, 127.0) as i8
}

#[inline]
fn signature_bucket(signature: u64, table: usize) -> u16 {
    ((signature >> (table * 13)) as u16) & SIGNATURE_ROUTING_MASK
}

fn embedding_signature_i8(values: &[i8]) -> u64 {
    embedding_signature(values.len(), |dimension| values[dimension] as f32)
}

fn embedding_signature_f32(values: &[f32]) -> u64 {
    embedding_signature(values.len(), |dimension| values[dimension])
}

fn embedding_signature(
    dimensions: usize,
    mut value_at: impl FnMut(usize) -> f32,
) -> u64 {
    let mut signature = 0u64;
    for bit in 0..64 {
        let mut state = 0x9e37_79b9_7f4a_7c15u64
            .wrapping_add((bit as u64).wrapping_mul(0xd1b5_4a32_d192_ed03))
            .wrapping_add(dimensions as u64);
        let mut projection = 0.0f32;
        for _ in 0..SIGNATURE_SAMPLES_PER_BIT {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let value = value_at((state as usize) % dimensions);
            projection += if state & (1 << 63) == 0 {
                value
            } else {
                -value
            };
        }
        if projection >= 0.0 {
            signature |= 1 << bit;
        }
    }
    signature
}

#[inline]
fn dot_i8_f32(left: &[i8], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: the runtime feature check covers every instruction used below.
        return unsafe { dot_i8_f32_avx2(left, right) };
    }
    dot_i8_f32_scalar(left, right)
}

#[inline]
fn dot_i8_f32_scalar(left: &[i8], right: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let mut index = 0usize;
    while index + 8 <= left.len() {
        sum += left[index] as f32 * right[index]
            + left[index + 1] as f32 * right[index + 1]
            + left[index + 2] as f32 * right[index + 2]
            + left[index + 3] as f32 * right[index + 3]
            + left[index + 4] as f32 * right[index + 4]
            + left[index + 5] as f32 * right[index + 5]
            + left[index + 6] as f32 * right[index + 6]
            + left[index + 7] as f32 * right[index + 7];
        index += 8;
    }
    while index < left.len() {
        sum += left[index] as f32 * right[index];
        index += 1;
    }
    sum
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_i8_f32_avx2(left: &[i8], right: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let mut accumulator = _mm256_setzero_ps();
    let mut index = 0usize;
    while index + 8 <= left.len() {
        let packed = _mm_loadl_epi64(left.as_ptr().add(index).cast::<__m128i>());
        let integers = _mm256_cvtepi8_epi32(packed);
        let values = _mm256_cvtepi32_ps(integers);
        let query = _mm256_loadu_ps(right.as_ptr().add(index));
        accumulator = _mm256_add_ps(accumulator, _mm256_mul_ps(values, query));
        index += 8;
    }

    let mut lanes = [0.0f32; 8];
    _mm256_storeu_ps(lanes.as_mut_ptr(), accumulator);
    let mut sum = lanes.into_iter().sum::<f32>();
    while index < left.len() {
        sum += left[index] as f32 * right[index];
        index += 1;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_rows_return_the_nearest_items_for_one_tenant() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(4, 8)).unwrap();
        let first = [1.0, 0.0, 0.0, 0.0];
        let second = [0.8, 0.2, 0.0, 0.0];
        let other_tenant = [1.0, 0.0, 0.0, 0.0];
        index
            .upsert_batch(&[
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        item_id: 10,
                    },
                    values: &first,
                },
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        item_id: 11,
                    },
                    values: &second,
                },
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 8,
                        item_id: 12,
                    },
                    values: &other_tenant,
                },
            ])
            .unwrap();

        let hits = index.search(7, &first, 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].key.item_id, 10);
        assert_eq!(hits[1].key.item_id, 11);
        assert!(hits[0].score > hits[1].score);
        assert_eq!(index.quantized_vector_bytes(), 3 * (4 + 4));
    }

    #[test]
    fn updates_reuse_the_existing_row_and_removal_keeps_positions_valid() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 4)).unwrap();
        let x = [1.0, 0.0];
        let y = [0.0, 1.0];
        let key_a = EmbeddingKey {
            tenant_id: 1,
            item_id: 1,
        };
        let key_b = EmbeddingKey {
            tenant_id: 1,
            item_id: 2,
        };
        index
            .upsert_batch(&[
                EmbeddingVector {
                    key: key_a,
                    values: &x,
                },
                EmbeddingVector {
                    key: key_b,
                    values: &y,
                },
            ])
            .unwrap();
        let (vector_address, vector_capacity) = {
            let rows = index.tenants.get(&1).unwrap();
            (rows.vectors.as_ptr() as usize, rows.vectors.capacity())
        };
        let report = index
            .upsert_batch(&[EmbeddingVector {
                key: key_a,
                values: &y,
            }])
            .unwrap();
        assert_eq!((report.inserted, report.updated), (0, 1));
        let rows = index.tenants.get(&1).unwrap();
        assert_eq!(rows.vectors.as_ptr() as usize, vector_address);
        assert_eq!(rows.vectors.capacity(), vector_capacity);
        assert!(index.remove(key_b));
        assert_eq!(index.search(1, &y, 2).unwrap()[0].key, key_a);
    }

    #[test]
    fn query_magnitude_does_not_change_compact_scores() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(3, 4)).unwrap();
        let stored = [9.0, 3.0, -1.0];
        let key = EmbeddingKey {
            tenant_id: 1,
            item_id: 7,
        };
        index
            .upsert_batch(&[EmbeddingVector {
                key,
                values: &stored,
            }])
            .unwrap();

        let unit_scale = index.search(1, &[3.0, 1.0, 0.0], 1).unwrap()[0];
        let large_scale = index.search(1, &[300.0, 100.0, 0.0], 1).unwrap()[0];
        assert_eq!(unit_scale.key, key);
        assert!((unit_scale.score - large_scale.score).abs() < 1e-6);
    }

    #[test]
    fn preparation_still_rejects_invalid_vector_values() {
        let index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 4)).unwrap();
        let key = EmbeddingKey {
            tenant_id: 1,
            item_id: 7,
        };
        assert_eq!(
            index
                .prepare_batch(&[EmbeddingVector {
                    key,
                    values: &[f32::NAN, 1.0],
                }])
                .unwrap_err(),
            EmbeddingIndexError::NonFiniteValue { key, dimension: 0 }
        );
        assert_eq!(
            index
                .prepare_batch(&[EmbeddingVector {
                    key,
                    values: &[0.0, 0.0],
                }])
                .unwrap_err(),
            EmbeddingIndexError::ZeroNorm { key }
        );
    }

    #[cfg(feature = "alloc-probe")]
    #[test]
    fn hot_update_and_candidate_scoring_add_no_query_temporary() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(4, 4)).unwrap();
        let key = EmbeddingKey {
            tenant_id: 1,
            item_id: 7,
        };
        let initial = [1.0, 0.5, 0.25, 0.125];
        let updated = [0.125, 0.25, 0.5, 1.0];
        index
            .upsert_batch(&[EmbeddingVector {
                key,
                values: &initial,
            }])
            .unwrap();

        let update_probe = crate::alloc_probe::Probe::start();
        index
            .upsert_batch(&[EmbeddingVector {
                key,
                values: &updated,
            }])
            .unwrap();
        let update_allocations = update_probe.stop();
        assert_eq!(update_allocations.allocs, 0);

        let query_probe = crate::alloc_probe::Probe::start();
        let hits = index
            .score_candidates(1, &[2.0, 4.0, 8.0, 16.0], &[7])
            .unwrap();
        let query_allocations = query_probe.stop();
        assert_eq!(hits.len(), 1);
        assert_eq!(query_allocations.allocs, 1);
    }
}
