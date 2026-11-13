// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};

use thiserror::Error;

const MAX_EMBEDDING_DIMENSIONS: usize = 4096;
const SIGNATURE_ROUTING_MIN_DIMENSIONS: usize = 384;
const SIGNATURE_ROUTING_MIN_ROWS: usize = 8192;
const SIGNATURE_ROUTING_TABLES: usize = 4;
const SIGNATURE_ROUTING_BITS: usize = 12;
const SIGNATURE_ROUTING_BUCKETS: usize = 1 << SIGNATURE_ROUTING_BITS;
const SIGNATURE_ROUTING_MASK: u16 = (1 << SIGNATURE_ROUTING_BITS) - 1;
const SIGNATURE_SAMPLES_PER_BIT: usize = 16;
const SIGNATURE_CANDIDATES_PER_HIT: usize = 128;
const SIGNATURE_MIN_CANDIDATES: usize = 4096;
const SIGNATURE_MAX_CANDIDATES: usize = 32_768;
const OWNER_EXACT_MAX_CANDIDATES: usize = 1_024;
const MAX_VECTOR_GROWTH_BYTES: usize = 1024 * 1024;
const OWNER_RANGE_MIN_ROWS: usize = 8192;
const OWNER_COUNT_PROBE_MAX_OWNERS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EmbeddingKey {
    pub tenant_id: u64,
    /// Application owner used to disambiguate item ids that are only unique within a node.
    pub owner_id: u64,
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

/// A validated query vector whose inverse norm can be reused across scoring passes.
#[derive(Debug, Clone, Copy)]
pub struct PreparedEmbeddingQuery<'a> {
    values: &'a [f32],
    inverse_norm: f32,
}

impl<'a> PreparedEmbeddingQuery<'a> {
    pub fn new(values: &'a [f32]) -> Result<Self, EmbeddingIndexError> {
        Ok(Self {
            values,
            inverse_norm: query_inverse_norm(values, values.len())?,
        })
    }

    pub fn values(self) -> &'a [f32] {
        self.values
    }

    pub fn dimensions(self) -> usize {
        self.values.len()
    }

    fn validate_dimensions(self, expected: usize) -> Result<(), EmbeddingIndexError> {
        if self.values.len() != expected {
            return Err(EmbeddingIndexError::QueryDimensionMismatch {
                expected,
                actual: self.values.len(),
            });
        }
        Ok(())
    }
}

/// Exact cosine scores for a bounded rerank set.
///
/// The compact index should select the set. This function is deliberately
/// allocation-light and does not own or retain application vectors.
pub fn score_embedding_candidates_exact(
    query: &[f32],
    candidates: &[EmbeddingVector<'_>],
) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
    score_prepared_embedding_candidates_exact(PreparedEmbeddingQuery::new(query)?, candidates)
}

pub fn score_prepared_embedding_candidates_exact(
    query: PreparedEmbeddingQuery<'_>,
    candidates: &[EmbeddingVector<'_>],
) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
    let mut hits = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        hits.push(score_prepared_embedding_candidate_exact(query, *candidate)?);
    }
    Ok(hits)
}

/// Exact cosine score for one already bounded candidate without allocating a result vector.
pub fn score_prepared_embedding_candidate_exact(
    query: PreparedEmbeddingQuery<'_>,
    candidate: EmbeddingVector<'_>,
) -> Result<EmbeddingHit, EmbeddingIndexError> {
    if candidate.values.len() != query.dimensions() {
        return Err(EmbeddingIndexError::DimensionMismatch {
            key: candidate.key,
            expected: query.dimensions(),
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
        dot += query.values[dimension] * value;
        norm_squared += value * value;
    }
    if norm_squared <= f32::EPSILON {
        return Err(EmbeddingIndexError::ZeroNorm { key: candidate.key });
    }
    Ok(EmbeddingHit {
        key: candidate.key,
        score: dot * query.inverse_norm * norm_squared.sqrt().recip(),
    })
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
    owner_ids: Vec<u64>,
    item_ids: Vec<u64>,
    positions: IdentityPositions,
    scales: Vec<f32>,
    vectors: Vec<i8>,
    signature_routing: Option<SignatureRouting>,
}

#[derive(Debug)]
enum IdentityPositions {
    Small(HashMap<(u64, u64), usize>),
    Ordered(BTreeMap<(u64, u64), usize>),
}

impl Default for IdentityPositions {
    fn default() -> Self {
        Self::Small(HashMap::new())
    }
}

impl IdentityPositions {
    fn promote_for(&mut self, additional: usize) {
        if !matches!(self, Self::Small(map) if map.len().saturating_add(additional) >= OWNER_RANGE_MIN_ROWS)
        {
            return;
        }
        let Self::Small(map) = std::mem::take(self) else {
            unreachable!()
        };
        *self = Self::Ordered(map.into_iter().collect());
    }

    fn reserve(&mut self, additional: usize) {
        self.promote_for(additional);
        if let Self::Small(map) = self {
            map.reserve(additional);
        }
    }

    fn get(&self, key: &(u64, u64)) -> Option<&usize> {
        match self {
            Self::Small(map) => map.get(key),
            Self::Ordered(map) => map.get(key),
        }
    }

    fn contains_key(&self, key: &(u64, u64)) -> bool {
        self.get(key).is_some()
    }

    fn insert(&mut self, key: (u64, u64), position: usize) -> Option<usize> {
        self.promote_for(1);
        match self {
            Self::Small(map) => map.insert(key, position),
            Self::Ordered(map) => map.insert(key, position),
        }
    }

    fn remove(&mut self, key: &(u64, u64)) -> Option<usize> {
        match self {
            Self::Small(map) => map.remove(key),
            Self::Ordered(map) => map.remove(key),
        }
    }

    fn ordered(&self) -> Option<&BTreeMap<(u64, u64), usize>> {
        match self {
            Self::Small(_) => None,
            Self::Ordered(map) => Some(map),
        }
    }
}

#[derive(Debug)]
struct SignatureRouting {
    // Tables retain compact row positions only; vectors stay in TenantRows.
    tables: [SignatureTable; SIGNATURE_ROUTING_TABLES],
}

#[derive(Debug)]
struct SignatureTable {
    heads: Box<[u32]>,
    next: Vec<u32>,
}

const NO_SIGNATURE_POSITION: u32 = u32::MAX;

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
            tables: std::array::from_fn(|_| SignatureTable::new(rows.item_ids.len())),
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
            self.tables[table].insert(signature_bucket(signature, table), position);
        }
    }

    fn remove(&mut self, signature: u64, position: usize) {
        let Ok(position) = u32::try_from(position) else {
            return;
        };
        for table in 0..SIGNATURE_ROUTING_TABLES {
            self.tables[table].remove(signature_bucket(signature, table), position);
        }
    }

    fn truncate(&mut self, rows: usize) {
        for table in &mut self.tables {
            table.next.truncate(rows);
        }
    }

    fn visit_candidates(
        &self,
        signature: u64,
        row_count: usize,
        budget: usize,
        mut visit: impl FnMut(u32),
    ) -> usize {
        let mut seen = SeenPositions::new(row_count, budget);
        let mut visited = 0usize;
        for distance in 0..=2 {
            for table in 0..SIGNATURE_ROUTING_TABLES {
                let bucket = signature_bucket(signature, table);
                match distance {
                    0 => {
                        if self.visit_bucket(
                            table,
                            bucket,
                            budget,
                            &mut seen,
                            &mut visited,
                            &mut visit,
                        ) {
                            return visited;
                        }
                    }
                    1 => {
                        for first in 0..SIGNATURE_ROUTING_BITS {
                            if self.visit_bucket(
                                table,
                                bucket ^ (1 << first),
                                budget,
                                &mut seen,
                                &mut visited,
                                &mut visit,
                            ) {
                                return visited;
                            }
                        }
                    }
                    2 => {
                        for first in 0..SIGNATURE_ROUTING_BITS {
                            for second in first + 1..SIGNATURE_ROUTING_BITS {
                                if self.visit_bucket(
                                    table,
                                    bucket ^ (1 << first) ^ (1 << second),
                                    budget,
                                    &mut seen,
                                    &mut visited,
                                    &mut visit,
                                ) {
                                    return visited;
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        visited
    }

    fn visit_bucket(
        &self,
        table: usize,
        bucket: u16,
        budget: usize,
        seen: &mut SeenPositions,
        visited: &mut usize,
        visit: &mut impl FnMut(u32),
    ) -> bool {
        self.tables[table].visit(bucket, |position| {
            if seen.insert(position) {
                visit(position);
                *visited += 1;
            }
            *visited < budget
        })
    }
}

impl SignatureTable {
    fn new(rows: usize) -> Self {
        Self {
            heads: vec![NO_SIGNATURE_POSITION; SIGNATURE_ROUTING_BUCKETS].into_boxed_slice(),
            next: vec![NO_SIGNATURE_POSITION; rows],
        }
    }

    fn insert(&mut self, bucket: u16, position: u32) {
        let position_index = position as usize;
        if position_index >= self.next.len() {
            self.next
                .resize(position_index.saturating_add(1), NO_SIGNATURE_POSITION);
        }
        let head = &mut self.heads[bucket as usize];
        self.next[position_index] = *head;
        *head = position;
    }

    fn remove(&mut self, bucket: u16, position: u32) {
        let bucket = bucket as usize;
        let position_index = position as usize;
        if position_index >= self.next.len() {
            return;
        }
        if self.heads[bucket] == position {
            self.heads[bucket] = self.next[position_index];
            self.next[position_index] = NO_SIGNATURE_POSITION;
            return;
        }

        let mut previous = self.heads[bucket];
        while previous != NO_SIGNATURE_POSITION {
            let previous_index = previous as usize;
            let current = self.next[previous_index];
            if current == position {
                self.next[previous_index] = self.next[position_index];
                self.next[position_index] = NO_SIGNATURE_POSITION;
                return;
            }
            previous = current;
        }
    }

    fn visit(&self, bucket: u16, mut visit: impl FnMut(u32) -> bool) -> bool {
        let mut position = self.heads[bucket as usize];
        while position != NO_SIGNATURE_POSITION {
            if !visit(position) {
                return true;
            }
            position = self.next[position as usize];
        }
        false
    }
}

impl TenantRows {
    fn reserve_vector_rows(&mut self, additional_rows: usize, dimensions: usize) {
        let required = self
            .vectors
            .len()
            .saturating_add(additional_rows.saturating_mul(dimensions));
        let capacity = self.vectors.capacity();
        if required <= capacity {
            return;
        }
        // Small buffers grow geometrically; large tenants retain bounded growth headroom.
        let growth = capacity.max(dimensions).min(MAX_VECTOR_GROWTH_BYTES);
        let target = required.max(capacity.saturating_add(growth));
        self.vectors
            .reserve_exact(target.saturating_sub(self.vectors.len()));
    }

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
            .is_some_and(|rows| rows.positions.contains_key(&(key.owner_id, key.item_id)))
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
        let identity = (input.key.owner_id, input.key.item_id);
        let (inserted, updated) = if let Some(&position) = rows.positions.get(&identity) {
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
            rows.reserve_vector_rows(1, self.config.dimensions);
            rows.owner_ids.push(input.key.owner_id);
            rows.item_ids.push(input.key.item_id);
            rows.positions.insert(identity, position);
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
        // Duplicate rejection is complete; do not retain its O(N) scratch beside the vectors.
        drop(batch_keys);

        let mut keys = Vec::with_capacity(batch.len());
        let mut scales = Vec::with_capacity(batch.len());
        let mut vectors = Vec::with_capacity(batch.len().saturating_mul(self.config.dimensions));
        for input in batch {
            keys.push(input.key);
            scales.push(quantize_unit_vector(input.key, input.values, &mut vectors)?);
        }
        Ok(CompactEmbeddingBatch {
            dimensions: self.config.dimensions,
            model_hash: self.config.model_hash,
            keys,
            scales,
            vectors,
        })
    }

    pub fn upsert_compact_batch(
        &mut self,
        batch: &CompactEmbeddingBatch,
    ) -> Result<EmbeddingWriteReport, EmbeddingIndexError> {
        self.upsert_compact_batch_inner(batch, false)
    }

    fn upsert_compact_batch_inner(
        &mut self,
        batch: &CompactEmbeddingBatch,
        all_missing: bool,
    ) -> Result<EmbeddingWriteReport, EmbeddingIndexError> {
        if batch.dimensions != self.config.dimensions {
            return Err(EmbeddingIndexError::DimensionMismatch {
                key: batch.keys.first().copied().unwrap_or(EmbeddingKey {
                    tenant_id: 0,
                    owner_id: 0,
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
        let mut new_entries_by_tenant = HashMap::<u64, usize>::new();
        for key in &batch.keys {
            if all_missing
                || !self
                    .tenants
                    .get(&key.tenant_id)
                    .is_some_and(|rows| {
                        rows.positions.contains_key(&(key.owner_id, key.item_id))
                    })
            {
                *new_entries_by_tenant.entry(key.tenant_id).or_default() += 1;
            }
        }
        let new_entries = new_entries_by_tenant.values().sum::<usize>();
        let requested = self.len().saturating_add(new_entries);
        if requested > self.config.max_entries {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: self.config.max_entries,
                requested,
            });
        }

        for (tenant_id, new_entries) in new_entries_by_tenant {
            let rows = self.tenants.entry(tenant_id).or_default();
            rows.owner_ids.reserve(new_entries);
            rows.item_ids.reserve(new_entries);
            rows.positions.reserve(new_entries);
            rows.scales.reserve(new_entries);
            rows.reserve_vector_rows(new_entries, self.config.dimensions);
        }

        let mut inserted = 0usize;
        let mut updated = 0usize;
        for row in 0..batch.len() {
            let key = batch.key(row);
            let scale = batch.scale(row);
            let vector = batch.vector(row);
            let rows = self.tenants.entry(key.tenant_id).or_default();
            let identity = (key.owner_id, key.item_id);
            if !all_missing {
                if let Some(&position) = rows.positions.get(&identity) {
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
                    continue;
                }
            }
            let position = rows.item_ids.len();
            rows.owner_ids.push(key.owner_id);
            rows.item_ids.push(key.item_id);
            rows.positions.insert(identity, position);
            rows.scales.push(scale);
            rows.vectors.extend_from_slice(vector);
            rows.refresh_signature_routing(self.config.dimensions, position, None);
            inserted += 1;
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
        all_missing: bool,
        protected: Option<&HashSet<EmbeddingKey>>,
    ) -> Result<EmbeddingWriteReport, EmbeddingIndexError> {
        if batch.len() > self.config.max_entries {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: self.config.max_entries,
                requested: batch.len(),
            });
        }
        let owned_protected = protected.is_none().then(|| {
            (0..batch.len())
                .map(|row| batch.key(row))
                .collect::<HashSet<_>>()
        });
        let protected = protected
            .or(owned_protected.as_ref())
            .expect("bounded insert always has protected keys");
        let new_entries = if all_missing {
            batch.len()
        } else {
            protected.iter().filter(|key| !self.contains(**key)).count()
        };
        let evict = self
            .len()
            .saturating_add(new_entries)
            .saturating_sub(self.config.max_entries);
        if evict > 0 {
            let victims = self
                .tenants
                .iter()
                .flat_map(|(tenant_id, rows)| {
                    rows.owner_ids.iter().zip(&rows.item_ids).map(
                        move |(owner_id, item_id)| EmbeddingKey {
                            tenant_id: *tenant_id,
                            owner_id: *owner_id,
                            item_id: *item_id,
                        },
                    )
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
        self.upsert_compact_batch_inner(batch, all_missing)
    }

    pub fn remove(&mut self, key: EmbeddingKey) -> bool {
        let Some(rows) = self.tenants.get_mut(&key.tenant_id) else {
            return false;
        };
        let Some(position) = rows.positions.remove(&(key.owner_id, key.item_id)) else {
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
            routing.truncate(last);
        }
        if position != last {
            let source = last * self.config.dimensions;
            let target = position * self.config.dimensions;
            rows.vectors
                .copy_within(source..source + self.config.dimensions, target);
        }
        rows.vectors
            .truncate(last.saturating_mul(self.config.dimensions));
        rows.owner_ids.swap_remove(position);
        rows.item_ids.swap_remove(position);
        rows.scales.swap_remove(position);
        if position != last {
            rows.positions.insert(
                (rows.owner_ids[position], rows.item_ids[position]),
                position,
            );
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
        self.search_prepared_rows(tenant_id, rows, PreparedEmbeddingQuery::new(query)?, limit)
    }

    pub(crate) fn search_prepared(
        &self,
        tenant_id: u64,
        query: PreparedEmbeddingQuery<'_>,
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(rows) = self.tenants.get(&tenant_id) else {
            return Ok(Vec::new());
        };
        self.search_prepared_rows(tenant_id, rows, query, limit)
    }

    /// Search selected application owners without materializing their vectors.
    /// A validated query can be reused across compatible embedding spaces.
    pub fn search_prepared_owners(
        &self,
        tenant_id: u64,
        query: PreparedEmbeddingQuery<'_>,
        limit: usize,
        owner_ids: &HashSet<u64>,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        if limit == 0 || owner_ids.is_empty() {
            return Ok(Vec::new());
        }
        let Some(rows) = self.tenants.get(&tenant_id) else {
            return Ok(Vec::new());
        };
        query.validate_dimensions(self.config.dimensions)?;
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if let Some(dot) = Avx2DotProduct::detect() {
            return Ok(self.search_prepared_owner_rows_with(
                &dot, tenant_id, rows, query, limit, owner_ids,
            ));
        }
        Ok(self.search_prepared_owner_rows_with(
            &ScalarDotProduct,
            tenant_id,
            rows,
            query,
            limit,
            owner_ids,
        ))
    }

    fn search_prepared_rows(
        &self,
        tenant_id: u64,
        rows: &TenantRows,
        query: PreparedEmbeddingQuery<'_>,
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        query.validate_dimensions(self.config.dimensions)?;
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if let Some(dot) = Avx2DotProduct::detect() {
            return Ok(self.search_prepared_rows_with(&dot, tenant_id, rows, query, limit));
        }
        Ok(self.search_prepared_rows_with(&ScalarDotProduct, tenant_id, rows, query, limit))
    }

    fn search_prepared_rows_with<D: CompactDotProduct>(
        &self,
        dot: &D,
        tenant_id: u64,
        rows: &TenantRows,
        query: PreparedEmbeddingQuery<'_>,
        limit: usize,
    ) -> Vec<EmbeddingHit> {
        let limit = limit.min(rows.item_ids.len());
        // Large tenants route first, then reuse the exact compact scorer on a bounded set.
        if let Some(routing) = &rows.signature_routing {
            let candidate_budget = limit
                .saturating_mul(SIGNATURE_CANDIDATES_PER_HIT)
                .clamp(SIGNATURE_MIN_CANDIDATES, SIGNATURE_MAX_CANDIDATES)
                .min(rows.item_ids.len());
            // Score while walking the buckets. The routed set can be 32K positions, and keeping
            // that set in a second vector only delays the same bounded-heap work while retaining
            // 128 KiB of query scratch at the configured ceiling.
            let mut heap = BinaryHeap::<Reverse<HeapHit>>::with_capacity(limit);
            let routed_count = routing.visit_candidates(
                embedding_signature_f32(query.values),
                rows.item_ids.len(),
                candidate_budget,
                |position| {
                    self.score_position(
                        dot,
                        rows,
                        query.values,
                        query.inverse_norm,
                        limit,
                        position as usize,
                        &mut heap,
                    );
                },
            );
            let minimum_routed_candidates = limit.saturating_mul(4).min(rows.item_ids.len());
            if routed_count >= minimum_routed_candidates {
                return Self::finish_search(tenant_id, rows, heap);
            }
        }
        self.search_positions(
            dot,
            tenant_id,
            rows,
            query.values,
            query.inverse_norm,
            limit,
            0..rows.item_ids.len(),
        )
    }

    fn search_prepared_owner_rows_with<D: CompactDotProduct>(
        &self,
        dot: &D,
        tenant_id: u64,
        rows: &TenantRows,
        query: PreparedEmbeddingQuery<'_>,
        limit: usize,
        owner_ids: &HashSet<u64>,
    ) -> Vec<EmbeddingHit> {
        let limit = limit.min(rows.item_ids.len());
        let Some(positions) = rows.positions.ordered() else {
            return self.search_positions(
                dot,
                tenant_id,
                rows,
                query.values,
                query.inverse_norm,
                limit,
                (0..rows.item_ids.len())
                    .filter(|position| owner_ids.contains(&rows.owner_ids[*position])),
            );
        };
        if let (Some((first, _)), Some((last, _))) =
            (positions.first_key_value(), positions.last_key_value())
        {
            if first.0 == last.0 && owner_ids.contains(&first.0) {
                return self.search_prepared_rows_with(dot, tenant_id, rows, query, limit);
            }
        }
        let candidate_budget = limit
            .saturating_mul(SIGNATURE_CANDIDATES_PER_HIT)
            .clamp(SIGNATURE_MIN_CANDIDATES, SIGNATURE_MAX_CANDIDATES)
            .min(rows.item_ids.len());
        let exact_budget = candidate_budget.min(OWNER_EXACT_MAX_CANDIDATES);
        if rows.signature_routing.is_none()
            || Self::prefer_exact_owner_scope(rows, owner_ids, exact_budget)
        {
            return self.search_positions(
                dot,
                tenant_id,
                rows,
                query.values,
                query.inverse_norm,
                limit,
                Self::owner_positions(rows, owner_ids),
            );
        }
        if let Some(routing) = &rows.signature_routing {
            let mut heap = BinaryHeap::<Reverse<HeapHit>>::with_capacity(limit);
            let mut matched = 0usize;
            routing.visit_candidates(
                embedding_signature_f32(query.values),
                rows.item_ids.len(),
                candidate_budget,
                |position| {
                    let position = position as usize;
                    if owner_ids.contains(&rows.owner_ids[position]) {
                        matched = matched.saturating_add(1);
                        self.score_position(
                            dot,
                            rows,
                            query.values,
                            query.inverse_norm,
                            limit,
                            position,
                            &mut heap,
                        );
                    }
                },
            );
            if matched >= limit.saturating_mul(4) {
                return Self::finish_search(tenant_id, rows, heap);
            }
        }

        self.search_positions(
            dot,
            tenant_id,
            rows,
            query.values,
            query.inverse_norm,
            limit,
            Self::owner_positions(rows, owner_ids),
        )
    }

    fn prefer_exact_owner_scope(
        rows: &TenantRows,
        owner_ids: &HashSet<u64>,
        budget: usize,
    ) -> bool {
        if owner_ids.len() <= OWNER_COUNT_PROBE_MAX_OWNERS {
            return Self::owner_positions(rows, owner_ids)
                .take(budget.saturating_add(1))
                .count()
                <= budget;
        }
        if owner_ids.len() > budget {
            return false;
        }
        let Some(positions) = rows.positions.ordered() else {
            return false;
        };
        owner_ids.iter().all(|owner| {
            let mut range = positions.range((*owner, 0)..=(*owner, u64::MAX));
            range.next();
            range.next().is_none()
        })
    }

    fn owner_positions<'a>(
        rows: &'a TenantRows,
        owner_ids: &'a HashSet<u64>,
    ) -> impl Iterator<Item = usize> + 'a {
        owner_ids.iter().flat_map(move |owner| {
            rows.positions
                .ordered()
                .into_iter()
                .flat_map(move |positions| positions.range((*owner, 0)..=(*owner, u64::MAX)))
                .map(|(_, position)| *position)
        })
    }

    fn search_positions<D: CompactDotProduct>(
        &self,
        dot: &D,
        tenant_id: u64,
        rows: &TenantRows,
        query: &[f32],
        query_inverse_norm: f32,
        limit: usize,
        positions: impl IntoIterator<Item = usize>,
    ) -> Vec<EmbeddingHit> {
        let mut heap = BinaryHeap::<Reverse<HeapHit>>::with_capacity(limit);

        for position in positions {
            self.score_position(
                dot,
                rows,
                query,
                query_inverse_norm,
                limit,
                position,
                &mut heap,
            );
        }

        Self::finish_search(tenant_id, rows, heap)
    }

    fn score_position<D: CompactDotProduct>(
        &self,
        dot: &D,
        rows: &TenantRows,
        query: &[f32],
        query_inverse_norm: f32,
        limit: usize,
        position: usize,
        heap: &mut BinaryHeap<Reverse<HeapHit>>,
    ) {
        let start = position * self.config.dimensions;
        let row = &rows.vectors[start..start + self.config.dimensions];
        let score =
            (dot.dot(row, query) * rows.scales[position] * query_inverse_norm).clamp(-1.0, 1.0);
        let candidate = HeapHit { score, position };
        if heap.len() < limit {
            heap.push(Reverse(candidate));
        } else if heap.peek().is_some_and(|smallest| candidate > smallest.0) {
            heap.pop();
            heap.push(Reverse(candidate));
        }
    }

    fn finish_search(
        tenant_id: u64,
        rows: &TenantRows,
        heap: BinaryHeap<Reverse<HeapHit>>,
    ) -> Vec<EmbeddingHit> {
        let mut hits: Vec<EmbeddingHit> = heap
            .into_iter()
            .map(|Reverse(hit)| EmbeddingHit {
                key: EmbeddingKey {
                    tenant_id,
                    owner_id: rows.owner_ids[hit.position],
                    item_id: rows.item_ids[hit.position],
                },
                score: hit.score,
            })
            .collect();
        hits.sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.key.owner_id.cmp(&right.key.owner_id))
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
        self.score_prepared_candidate_ids_for_rows(
            tenant_id,
            rows,
            PreparedEmbeddingQuery::new(query)?,
            candidate_item_ids,
            candidate_count,
        )
    }

    fn score_prepared_candidate_ids_for_rows(
        &self,
        tenant_id: u64,
        rows: &TenantRows,
        query: PreparedEmbeddingQuery<'_>,
        candidate_item_ids: impl IntoIterator<Item = u64>,
        candidate_count: usize,
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        let mut hits = Vec::with_capacity(candidate_count.min(rows.item_ids.len()));
        self.visit_prepared_candidate_ids_for_rows(
            tenant_id,
            rows,
            query,
            candidate_item_ids,
            |hit| hits.push(hit),
        )?;
        Ok(hits)
    }

    pub(crate) fn visit_prepared_candidate_ids(
        &self,
        tenant_id: u64,
        query: PreparedEmbeddingQuery<'_>,
        candidate_item_ids: impl IntoIterator<Item = u64>,
        visit: impl FnMut(EmbeddingHit),
    ) -> Result<usize, EmbeddingIndexError> {
        let Some(rows) = self.tenants.get(&tenant_id) else {
            return Ok(0);
        };
        self.visit_prepared_candidate_ids_for_rows(
            tenant_id,
            rows,
            query,
            candidate_item_ids,
            visit,
        )
    }

    fn visit_prepared_candidate_ids_for_rows(
        &self,
        tenant_id: u64,
        rows: &TenantRows,
        query: PreparedEmbeddingQuery<'_>,
        candidate_item_ids: impl IntoIterator<Item = u64>,
        visit: impl FnMut(EmbeddingHit),
    ) -> Result<usize, EmbeddingIndexError> {
        query.validate_dimensions(self.config.dimensions)?;
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if let Some(dot) = Avx2DotProduct::detect() {
            return Ok(self.visit_prepared_candidate_ids_for_rows_with(
                &dot,
                tenant_id,
                rows,
                query,
                candidate_item_ids,
                visit,
            ));
        }
        Ok(self.visit_prepared_candidate_ids_for_rows_with(
            &ScalarDotProduct,
            tenant_id,
            rows,
            query,
            candidate_item_ids,
            visit,
        ))
    }

    fn visit_prepared_candidate_ids_for_rows_with<D: CompactDotProduct>(
        &self,
        dot: &D,
        tenant_id: u64,
        rows: &TenantRows,
        query: PreparedEmbeddingQuery<'_>,
        candidate_item_ids: impl IntoIterator<Item = u64>,
        mut visit: impl FnMut(EmbeddingHit),
    ) -> usize {
        let mut visited = 0usize;
        for item_id in candidate_item_ids {
            let Some(&position) = rows.positions.get(&(0, item_id)) else {
                continue;
            };
            let start = position * self.config.dimensions;
            let row = &rows.vectors[start..start + self.config.dimensions];
            visit(EmbeddingHit {
                key: EmbeddingKey {
                    tenant_id,
                    owner_id: 0,
                    item_id,
                },
                score: (dot.dot(row, query.values) * rows.scales[position] * query.inverse_norm)
                    .clamp(-1.0, 1.0),
            });
            visited = visited.saturating_add(1);
        }
        visited
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

/// Selected once per search so row scoring does not repeat CPU feature detection.
trait CompactDotProduct {
    fn dot(&self, left: &[i8], right: &[f32]) -> f32;
}

struct ScalarDotProduct;

impl CompactDotProduct for ScalarDotProduct {
    #[inline(always)]
    fn dot(&self, left: &[i8], right: &[f32]) -> f32 {
        dot_i8_f32_scalar(left, right)
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
struct Avx2DotProduct {
    _feature_detected: (),
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
impl Avx2DotProduct {
    fn detect() -> Option<Self> {
        std::arch::is_x86_feature_detected!("avx2").then_some(Self {
            _feature_detected: (),
        })
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
impl CompactDotProduct for Avx2DotProduct {
    #[inline(always)]
    fn dot(&self, left: &[i8], right: &[f32]) -> f32 {
        // SAFETY: an instance exists only after a successful runtime AVX2 check.
        unsafe { dot_i8_f32_avx2(left, right) }
    }
}

#[inline]
fn dot_i8_f32_scalar(left: &[i8], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
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
    debug_assert_eq!(left.len(), right.len());
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
    fn one_candidate_exact_score_matches_the_batch_scorer() {
        let query_values = [1.0, 2.0, 3.0, 4.0];
        let candidate_values = [4.0, 3.0, 2.0, 1.0];
        let query = PreparedEmbeddingQuery::new(&query_values).unwrap();
        let candidate = EmbeddingVector {
            key: EmbeddingKey {
                tenant_id: 7,
                owner_id: 0,
                item_id: 11,
            },
            values: &candidate_values,
        };
        let one = score_prepared_embedding_candidate_exact(query, candidate).unwrap();
        let batch = score_prepared_embedding_candidates_exact(query, &[candidate]).unwrap();
        assert_eq!(batch, vec![one]);
    }

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
                        owner_id: 0,
                        item_id: 10,
                    },
                    values: &first,
                },
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        owner_id: 0,
                        item_id: 11,
                    },
                    values: &second,
                },
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 8,
                        owner_id: 0,
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
    fn equal_item_ids_from_different_owners_remain_distinct() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 4)).unwrap();
        let first = [1.0, 0.0];
        let second = [0.0, 1.0];
        index
            .upsert_batch(&[
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        owner_id: 10,
                        item_id: 99,
                    },
                    values: &first,
                },
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        owner_id: 20,
                        item_id: 99,
                    },
                    values: &second,
                },
            ])
            .unwrap();

        let hits = index.search(7, &first, 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!((hits[0].key.owner_id, hits[0].key.item_id), (10, 99));
        assert_eq!((hits[1].key.owner_id, hits[1].key.item_id), (20, 99));
    }

    #[test]
    fn owner_filtered_search_ignores_better_scoring_rows_from_other_nodes() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 8)).unwrap();
        let query = [1.0, 0.0];
        let selected = [0.8, 0.2];
        let distractor = [1.0, 0.0];
        index
            .upsert_batch(&[
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        owner_id: 10,
                        item_id: 1,
                    },
                    values: &selected,
                },
                EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        owner_id: 20,
                        item_id: 2,
                    },
                    values: &distractor,
                },
            ])
            .unwrap();

        assert_eq!(index.search(7, &query, 1).unwrap()[0].key.owner_id, 20);
        let owners = HashSet::from([10]);
        let hits = index
            .search_prepared_owners(
                7,
                PreparedEmbeddingQuery::new(&query).unwrap(),
                1,
                &owners,
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!((hits[0].key.owner_id, hits[0].key.item_id), (10, 1));
    }

    #[test]
    fn updates_reuse_the_existing_row_and_removal_keeps_positions_valid() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 4)).unwrap();
        let x = [1.0, 0.0];
        let y = [0.0, 1.0];
        let key_a = EmbeddingKey {
            tenant_id: 1,
            owner_id: 0,
            item_id: 1,
        };
        let key_b = EmbeddingKey {
            tenant_id: 1,
            owner_id: 0,
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
            owner_id: 0,
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
            owner_id: 0,
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
        assert_eq!(
            index
                .prepare_batch(&[
                    EmbeddingVector {
                        key,
                        values: &[1.0, 0.0],
                    },
                    EmbeddingVector {
                        key,
                        values: &[0.0, 1.0],
                    },
                ])
                .unwrap_err(),
            EmbeddingIndexError::DuplicateKey { key }
        );
    }

    #[test]
    fn owner_ranges_match_exact_scoring_with_extreme_identity_values() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 16)).unwrap();
        for owner_id in [0, 1, u64::MAX] {
            for item_id in [0, u64::MAX] {
                index
                    .upsert_batch(&[EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 7,
                            owner_id,
                            item_id,
                        },
                        values: &[1.0, if item_id == 0 { 0.5 } else { -0.5 }],
                    }])
                    .unwrap();
            }
        }
        let owners = HashSet::from([1, u64::MAX]);
        index
            .tenants
            .get_mut(&7)
            .unwrap()
            .positions
            .reserve(OWNER_RANGE_MIN_ROWS);
        let rows = &index.tenants[&7];
        let query = PreparedEmbeddingQuery::new(&[1.0, 0.25]).unwrap();
        let expected = index.search_positions(
            &ScalarDotProduct,
            7,
            rows,
            query.values,
            query.inverse_norm,
            3,
            (0..rows.item_ids.len()).filter(|position| owners.contains(&rows.owner_ids[*position])),
        );
        let actual =
            index.search_prepared_owner_rows_with(&ScalarDotProduct, 7, rows, query, 3, &owners);
        assert_eq!(actual, expected);
        assert_eq!(EmbeddingIndex::owner_positions(rows, &owners).count(), 4);
        assert!(index
            .search_prepared_owners(8, query, 3, &owners)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn owner_scope_probe_skips_dense_multi_owner_counts() {
        let mut rows = TenantRows::default();
        rows.positions.reserve(OWNER_RANGE_MIN_ROWS);
        for owner in 0..32 {
            for item in 0..100 {
                rows.positions.insert((owner, item), 0);
            }
            rows.positions.insert((128 + owner, 0), 0);
        }
        assert!(EmbeddingIndex::prefer_exact_owner_scope(
            &rows,
            &HashSet::from([0]),
            1024
        ));
        assert!(!EmbeddingIndex::prefer_exact_owner_scope(
            &rows,
            &(0..32).collect(),
            1024
        ));
        assert!(EmbeddingIndex::prefer_exact_owner_scope(
            &rows,
            &(128..160).collect(),
            1024
        ));
        assert!(!EmbeddingIndex::prefer_exact_owner_scope(
            &rows,
            &(128..160).collect(),
            16
        ));
    }

    #[test]
    fn identity_index_promotes_without_losing_rows() {
        let mut positions = IdentityPositions::default();
        for item in 0..OWNER_RANGE_MIN_ROWS - 1 {
            positions.insert(((item % 8) as u64, item as u64), item);
        }
        assert!(matches!(positions, IdentityPositions::Small(_)));
        let last = OWNER_RANGE_MIN_ROWS - 1;
        positions.insert(((last % 8) as u64, last as u64), last);
        assert_eq!(positions.ordered().unwrap().len(), OWNER_RANGE_MIN_ROWS);
        for item in 0..OWNER_RANGE_MIN_ROWS {
            assert_eq!(
                positions.get(&((item % 8) as u64, item as u64)),
                Some(&item)
            );
        }
        assert_eq!(positions.remove(&(0, 0)), Some(0));
        assert!(!positions.contains_key(&(0, 0)));
        positions.insert((u64::MAX, u64::MAX), 7);
        assert_eq!(positions.get(&(u64::MAX, u64::MAX)), Some(&7));
        let mut reserved = IdentityPositions::default();
        reserved.insert((4, 7), 1);
        reserved.reserve(OWNER_RANGE_MIN_ROWS);
        assert_eq!(reserved.ordered().unwrap().get(&(4, 7)), Some(&1));
    }

    #[test]
    fn vector_growth_keeps_bounded_spare_capacity() {
        let mut rows = TenantRows::default();
        let dimensions = 384;
        let mut previous = 0;
        for count in [1, 8, 512, 2_048, 8_192, 100_000] {
            rows.reserve_vector_rows(count - previous, dimensions);
            rows.vectors.resize(count * dimensions, 0);
            if count == 1 {
                assert!(rows.vectors.capacity() <= dimensions * 2);
            }
            assert!(rows.vectors.capacity() - rows.vectors.len() <= MAX_VECTOR_GROWTH_BYTES);
            previous = count;
        }
        let capacity = rows.vectors.capacity();
        rows.reserve_vector_rows(0, dimensions);
        assert_eq!(rows.vectors.capacity(), capacity);
    }

    #[test]
    fn uniform_owner_filters_preserve_unfiltered_ranking() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 8)).unwrap();
        for item_id in 0..4 {
            index
                .upsert_batch(&[EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        owner_id: 42,
                        item_id,
                    },
                    values: &[1.0, item_id as f32],
                }])
                .unwrap();
        }
        index
            .tenants
            .get_mut(&7)
            .unwrap()
            .positions
            .reserve(OWNER_RANGE_MIN_ROWS);
        let query = PreparedEmbeddingQuery::new(&[1.0, 0.25]).unwrap();
        let expected = index.search_prepared(7, query, 3).unwrap();
        let hits = index
            .search_prepared_owners(7, query, 3, &HashSet::from([42, 99]))
            .unwrap();
        assert_eq!(hits, expected);
        assert!(index
            .search_prepared_owners(7, query, 3, &HashSet::from([99]))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn owner_ranges_follow_updates_and_swap_removals() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 16)).unwrap();
        let keys: Vec<_> = (0..6)
            .map(|item_id| EmbeddingKey {
                tenant_id: 7,
                owner_id: item_id % 3,
                item_id,
            })
            .collect();
        for key in &keys {
            index
                .upsert_batch(&[EmbeddingVector {
                    key: *key,
                    values: &[1.0, 0.0],
                }])
                .unwrap();
        }
        index
            .tenants
            .get_mut(&7)
            .unwrap()
            .positions
            .reserve(OWNER_RANGE_MIN_ROWS);
        index.remove(keys[0]);
        index.remove(keys[2]);
        index
            .upsert_batch(&[EmbeddingVector {
                key: keys[1],
                values: &[0.0, 1.0],
            }])
            .unwrap();
        for owner in 0..4 {
            let owners = HashSet::from([owner]);
            let rows = &index.tenants[&7];
            let positions: Vec<_> = EmbeddingIndex::owner_positions(rows, &owners).collect();
            assert_eq!(
                positions.len(),
                rows.owner_ids.iter().filter(|id| **id == owner).count()
            );
            for position in positions {
                assert_eq!(rows.owner_ids[position], owner);
                assert_eq!(
                    *rows
                        .positions
                        .get(&(owner, rows.item_ids[position]))
                        .unwrap(),
                    position
                );
            }
        }
        let query = PreparedEmbeddingQuery::new(&[0.0, 1.0]).unwrap();
        let hits = index
            .search_prepared_owners(7, query, 2, &HashSet::from([1]))
            .unwrap();
        assert_eq!(hits[0].key, keys[1]);
    }

    #[test]
    fn routed_search_streams_the_same_hits_as_a_full_scan() {
        let dimensions = SIGNATURE_ROUTING_MIN_DIMENSIONS;
        let row_count = 256;
        let mut rows = TenantRows {
            owner_ids: vec![0; row_count],
            item_ids: (0..row_count as u64).collect(),
            positions: IdentityPositions::default(),
            scales: vec![1.0 / 127.0; row_count],
            vectors: vec![1; row_count * dimensions],
            signature_routing: None,
        };
        rows.signature_routing = Some(SignatureRouting::build(&rows, dimensions));
        let mut index =
            EmbeddingIndex::new(EmbeddingIndexConfig::new(dimensions, row_count)).unwrap();
        index.total_entries = row_count;
        index.tenants.insert(7, rows);
        let query = vec![1.0; dimensions];
        let prepared = PreparedEmbeddingQuery::new(&query).unwrap();
        let rows = index.tenants.get(&7).unwrap();
        let expected = index.search_positions(
            &ScalarDotProduct,
            7,
            rows,
            prepared.values,
            prepared.inverse_norm,
            16,
            0..row_count,
        );

        assert_eq!(index.search(7, &query, 16).unwrap(), expected);
    }

    #[test]
    fn signature_table_keeps_mutable_buckets_in_contiguous_storage() {
        let mut table = SignatureTable::new(4);
        assert_eq!(table.heads.len(), SIGNATURE_ROUTING_BUCKETS);
        assert_eq!(table.next.len(), 4);

        for position in 0..4 {
            table.insert(17, position);
        }
        table.remove(17, 3);
        table.remove(17, 1);
        table.remove(17, 0);

        let mut positions = Vec::new();
        assert!(!table.visit(17, |position| {
            positions.push(position);
            true
        }));
        assert_eq!(positions, vec![2]);

        table.insert(17, 1);
        positions.clear();
        assert!(!table.visit(17, |position| {
            positions.push(position);
            true
        }));
        assert_eq!(positions, vec![1, 2]);
    }

    #[cfg(feature = "alloc-probe")]
    #[test]
    fn hot_update_and_candidate_scoring_add_no_query_temporary() {
        let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(4, 4)).unwrap();
        let key = EmbeddingKey {
            tenant_id: 1,
            owner_id: 0,
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
