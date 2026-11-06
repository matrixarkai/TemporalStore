// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};

use thiserror::Error;

const MAX_EMBEDDING_DIMENSIONS: usize = 4096;

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
            rows.scales[position] = scale;
            let start = position * self.config.dimensions;
            quantize_into(
                input.values,
                quantization_multiplier,
                &mut rows.vectors[start..start + self.config.dimensions],
            );
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
        let mut batch_keys = HashSet::with_capacity(batch.len());
        let mut new_entries = 0usize;
        for input in batch {
            self.validate_vector_dimensions(input.key, input.values)?;
            if !batch_keys.insert(input.key) {
                return Err(EmbeddingIndexError::DuplicateKey { key: input.key });
            }
            let exists = self
                .tenants
                .get(&input.key.tenant_id)
                .is_some_and(|rows| rows.positions.contains_key(&input.key.item_id));
            if !exists {
                new_entries += 1;
            }
        }

        let requested = self.len().saturating_add(new_entries);
        if requested > self.config.max_entries {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: self.config.max_entries,
                requested,
            });
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
                rows.scales[position] = scale;
                let start = position * self.config.dimensions;
                rows.vectors[start..start + self.config.dimensions].copy_from_slice(vector);
                updated += 1;
            } else {
                let position = rows.item_ids.len();
                rows.item_ids.push(key.item_id);
                rows.positions.insert(key.item_id, position);
                rows.scales.push(scale);
                rows.vectors.extend_from_slice(vector);
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

    pub fn remove(&mut self, key: EmbeddingKey) -> bool {
        let Some(rows) = self.tenants.get_mut(&key.tenant_id) else {
            return false;
        };
        let Some(position) = rows.positions.remove(&key.item_id) else {
            return false;
        };
        let last = rows.item_ids.len() - 1;
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
        let mut heap = BinaryHeap::<Reverse<HeapHit>>::with_capacity(limit);

        for position in 0..rows.item_ids.len() {
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
        Ok(hits)
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
        let Some(rows) = self.tenants.get(&tenant_id) else {
            return Ok(Vec::new());
        };
        let query_inverse_norm = query_inverse_norm(query, self.config.dimensions)?;
        let mut hits = Vec::with_capacity(candidate_item_ids.len().min(rows.item_ids.len()));
        for item_id in candidate_item_ids {
            let Some(&position) = rows.positions.get(item_id) else {
                continue;
            };
            let start = position * self.config.dimensions;
            let row = &rows.vectors[start..start + self.config.dimensions];
            hits.push(EmbeddingHit {
                key: EmbeddingKey {
                    tenant_id,
                    item_id: *item_id,
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
fn dot_i8_f32(left: &[i8], right: &[f32]) -> f32 {
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
