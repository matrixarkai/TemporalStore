// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, VecDeque};

use super::{
    EmbeddingHit, EmbeddingIndex, EmbeddingIndexConfig, EmbeddingIndexError, EmbeddingKey,
    EmbeddingVector, EmbeddingWriteReport,
};

const DEFAULT_MAX_CACHED_ENTRIES: usize = 1_000_000;
const DEFAULT_MAX_SPACES: usize = 8;
const DEFAULT_MAX_CACHED_VECTOR_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmbeddingChannel {
    Node,
    Summary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EmbeddingSpace {
    channel: EmbeddingChannel,
    model_hash: u64,
    dimensions: usize,
}

/// Process-local compact indexes separated by encoder and semantic owner.
///
/// Keeping spaces independent prevents vectors from different encoders from
/// sharing a dot product. Tenant rows remain isolated inside each index.
/// A global entry budget bounds row metadata, a global byte budget bounds the
/// compact vector payload across every space, and a separate space bound
/// prevents sparse historical model versions from accumulating maps and
/// allocation overhead.
#[derive(Debug)]
pub struct EmbeddingIndexRegistry {
    max_entries: usize,
    max_spaces: usize,
    max_vector_bytes: usize,
    spaces: HashMap<EmbeddingSpace, EmbeddingIndex>,
    space_order: VecDeque<EmbeddingSpace>,
}

impl Default for EmbeddingIndexRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CACHED_ENTRIES)
    }
}

impl EmbeddingIndexRegistry {
    /// Creates a registry whose entry limit is shared by all embedding spaces.
    pub fn new(max_entries: usize) -> Self {
        Self::with_limits(max_entries, DEFAULT_MAX_SPACES)
    }

    /// Creates a registry with explicit global entry and embedding-space limits.
    ///
    /// Both limits are clamped to one so cache publication always has a usable target.
    pub fn with_limits(max_entries: usize, max_spaces: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            max_spaces: max_spaces.max(1),
            max_vector_bytes: DEFAULT_MAX_CACHED_VECTOR_BYTES,
            spaces: HashMap::new(),
            space_order: VecDeque::new(),
        }
    }

    fn max_entries_for_dimensions(&self, dimensions: usize) -> usize {
        let bytes_per_entry = dimensions.saturating_add(std::mem::size_of::<f32>());
        self.max_entries
            .min(self.max_vector_bytes / bytes_per_entry.max(1))
            .max(1)
    }

    fn quantized_vector_bytes(&self) -> usize {
        self.spaces
            .values()
            .map(EmbeddingIndex::quantized_vector_bytes)
            .sum()
    }

    fn upsert(
        &mut self,
        channel: EmbeddingChannel,
        model_hash: u64,
        batch: &[EmbeddingVector<'_>],
        only_missing: bool,
    ) -> Result<Option<EmbeddingWriteReport>, EmbeddingIndexError> {
        let Some(first) = batch.first() else {
            return Ok(None);
        };
        let space = EmbeddingSpace {
            channel,
            model_hash,
            dimensions: first.values.len(),
        };
        let space_entry_limit = self.max_entries_for_dimensions(space.dimensions);
        let mut missing = Vec::new();
        let rows = if !only_missing {
            batch
        } else if let Some(index) = self.spaces.get(&space) {
            if batch.iter().all(|row| !index.contains(row.key)) {
                // A cold chunk can be quantized directly from the caller's borrowed rows.
                batch
            } else {
                missing.extend(batch.iter().copied().filter(|row| !index.contains(row.key)));
                missing.as_slice()
            }
        } else {
            batch
        };
        let compact = if let Some(index) = self.spaces.get(&space) {
            index.prepare_batch_unbounded(rows)?
        } else {
            EmbeddingIndex::new(
                EmbeddingIndexConfig::new(space.dimensions, space_entry_limit)
                    .with_model_hash(model_hash),
            )?
            .prepare_batch_unbounded(rows)?
        };
        if compact.len() > space_entry_limit {
            return Err(EmbeddingIndexError::CapacityExceeded {
                capacity: space_entry_limit,
                requested: compact.len(),
            });
        }

        for (existing_space, index) in &mut self.spaces {
            if existing_space.channel == channel && *existing_space != space {
                for row in batch {
                    index.remove(row.key);
                }
            }
        }
        self.prune_empty_spaces();
        if compact.is_empty() {
            return Ok(None);
        }

        if !self.spaces.contains_key(&space) {
            while self.spaces.len() >= self.max_spaces {
                if self.evict_oldest_space_except(space).is_none() {
                    break;
                }
            }
            self.spaces.insert(
                space,
                EmbeddingIndex::new(
                    EmbeddingIndexConfig::new(space.dimensions, space_entry_limit)
                        .with_model_hash(model_hash),
                )?,
            );
        }

        let new_entries = {
            let index = self.spaces.get(&space).expect("embedding space exists");
            (0..compact.len())
                .filter(|row| !index.contains(compact.key(*row)))
                .count()
        };
        let current_entries = self
            .spaces
            .get(&space)
            .expect("embedding space exists")
            .len();
        let retained_growth = new_entries.min(space_entry_limit.saturating_sub(current_entries));
        let retained_growth_bytes = retained_growth
            .saturating_mul(space.dimensions.saturating_add(std::mem::size_of::<f32>()));
        while self.len().saturating_add(retained_growth) > self.max_entries
            || self
                .quantized_vector_bytes()
                .saturating_add(retained_growth_bytes)
                > self.max_vector_bytes
        {
            if self.evict_oldest_space_except(space).is_none() {
                break;
            }
        }
        let report = self
            .spaces
            .get_mut(&space)
            .expect("embedding space exists")
            .upsert_compact_batch_bounded(&compact)?;
        self.touch_space(space);
        Ok(Some(report))
    }

    fn prune_empty_spaces(&mut self) {
        self.spaces.retain(|_, index| !index.is_empty());
        let spaces = &self.spaces;
        self.space_order.retain(|space| spaces.contains_key(space));
    }

    fn touch_space(&mut self, space: EmbeddingSpace) {
        self.space_order.retain(|existing| *existing != space);
        self.space_order.push_back(space);
    }

    fn evict_oldest_space_except(&mut self, protected: EmbeddingSpace) -> Option<usize> {
        let evicted = if let Some(position) = self
            .space_order
            .iter()
            .position(|space| *space != protected)
        {
            self.space_order.remove(position)?
        } else {
            self.spaces
                .keys()
                .copied()
                .find(|space| *space != protected)?
        };
        self.space_order.retain(|space| *space != evicted);
        self.spaces.remove(&evicted).map(|index| index.len())
    }

    pub fn upsert_missing_batch(
        &mut self,
        channel: EmbeddingChannel,
        model_hash: u64,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<Option<EmbeddingWriteReport>, EmbeddingIndexError> {
        self.upsert(channel, model_hash, batch, true)
    }

    pub fn upsert_batch(
        &mut self,
        channel: EmbeddingChannel,
        model_hash: u64,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<Option<EmbeddingWriteReport>, EmbeddingIndexError> {
        self.upsert(channel, model_hash, batch, false)
    }

    pub fn score_candidates(
        &self,
        channel: EmbeddingChannel,
        tenant_id: u64,
        model_hash: u64,
        query: &[f32],
        candidate_item_ids: &[u64],
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        let mut matching = self.spaces.iter().filter(|(space, _)| {
            space.channel == channel
                && space.dimensions == query.len()
                && (model_hash == 0 || space.model_hash == 0 || space.model_hash == model_hash)
        });
        let Some((_, first)) = matching.next() else {
            return Ok(Vec::new());
        };
        let Some(second) = matching.next() else {
            return first.score_candidates(tenant_id, query, candidate_item_ids);
        };

        let mut hits = first.score_candidates(tenant_id, query, candidate_item_ids)?;
        let mut positions = hits
            .iter()
            .enumerate()
            .map(|(position, hit)| (hit.key.item_id, position))
            .collect::<HashMap<_, _>>();
        for (_, index) in std::iter::once(second).chain(matching) {
            for hit in index.score_candidates(tenant_id, query, candidate_item_ids)? {
                if let Some(&position) = positions.get(&hit.key.item_id) {
                    let existing: &mut EmbeddingHit = &mut hits[position];
                    existing.score = existing.score.max(hit.score);
                } else {
                    positions.insert(hit.key.item_id, hits.len());
                    hits.push(hit);
                }
            }
        }
        Ok(hits)
    }

    pub fn remove(&mut self, channel: EmbeddingChannel, key: EmbeddingKey) -> bool {
        let mut removed = false;
        for (space, index) in &mut self.spaces {
            if space.channel == channel {
                removed |= index.remove(key);
            }
        }
        self.prune_empty_spaces();
        removed
    }

    pub fn len(&self) -> usize {
        self.spaces.values().map(EmbeddingIndex::len).sum()
    }

    /// Quantized vector payload retained across all channels and embedding spaces.
    pub fn vector_bytes(&self) -> usize {
        self.quantized_vector_bytes()
    }

    pub fn is_empty(&self) -> bool {
        self.spaces.values().all(EmbeddingIndex::is_empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_scoring_is_tenant_model_and_channel_local() {
        let mut registry = EmbeddingIndexRegistry::new(16);
        let x = [1.0, 0.0];
        let y = [0.0, 1.0];
        registry
            .upsert_batch(
                EmbeddingChannel::Node,
                11,
                &[
                    EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 7,
                            item_id: 1,
                        },
                        values: &x,
                    },
                    EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 7,
                            item_id: 2,
                        },
                        values: &y,
                    },
                ],
            )
            .unwrap();
        registry
            .upsert_batch(
                EmbeddingChannel::Summary,
                11,
                &[EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        item_id: 1,
                    },
                    values: &y,
                }],
            )
            .unwrap();

        let node = registry
            .score_candidates(EmbeddingChannel::Node, 7, 11, &x, &[2, 1])
            .unwrap();
        assert_eq!(node.len(), 2);
        assert_eq!(node[0].key.item_id, 2);
        assert!(node[1].score > node[0].score);
        let summary = registry
            .score_candidates(EmbeddingChannel::Summary, 7, 11, &x, &[1])
            .unwrap();
        assert_eq!(summary[0].score, 0.0);
        assert!(registry
            .score_candidates(EmbeddingChannel::Node, 8, 11, &x, &[1, 2])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_model_update_removes_the_old_space_row() {
        let mut registry = EmbeddingIndexRegistry::new(4);
        let x = [1.0, 0.0];
        let y = [0.0, 1.0];
        let row = |values| EmbeddingVector {
            key: EmbeddingKey {
                tenant_id: 3,
                item_id: 9,
            },
            values,
        };
        registry
            .upsert_batch(EmbeddingChannel::Node, 1, &[row(&x)])
            .unwrap();
        registry
            .upsert_missing_batch(EmbeddingChannel::Node, 2, &[row(&y)])
            .unwrap();

        assert!(registry
            .score_candidates(EmbeddingChannel::Node, 3, 1, &x, &[9])
            .unwrap()
            .is_empty());
        assert_eq!(
            registry
                .score_candidates(EmbeddingChannel::Node, 3, 2, &y, &[9])
                .unwrap()[0]
                .key
                .item_id,
            9
        );
    }

    #[test]
    fn an_unknown_stored_model_remains_scoreable() {
        let mut registry = EmbeddingIndexRegistry::new(4);
        let x = [1.0, 0.0];
        registry
            .upsert_batch(
                EmbeddingChannel::Summary,
                0,
                &[EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 3,
                        item_id: 9,
                    },
                    values: &x,
                }],
            )
            .unwrap();

        let hits = registry
            .score_candidates(EmbeddingChannel::Summary, 3, 17, &x, &[9])
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.item_id, 9);
    }

    #[test]
    fn removing_a_key_clears_its_channel() {
        let mut registry = EmbeddingIndexRegistry::new(4);
        let x = [1.0, 0.0];
        let key = EmbeddingKey {
            tenant_id: 3,
            item_id: 9,
        };
        for model_hash in [0, 17] {
            registry
                .upsert_missing_batch(
                    EmbeddingChannel::Summary,
                    model_hash,
                    &[EmbeddingVector { key, values: &x }],
                )
                .unwrap();
        }

        assert!(registry.remove(EmbeddingChannel::Summary, key));
        assert!(registry
            .score_candidates(EmbeddingChannel::Summary, 3, 0, &x, &[9])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn full_space_recycles_a_row_for_new_embeddings() {
        let mut registry = EmbeddingIndexRegistry::new(2);
        let x = [1.0, 0.0];
        let y = [0.0, 1.0];
        let diagonal = [1.0, 1.0];
        for (item_id, values) in [(1, &x), (2, &y), (3, &diagonal)] {
            registry
                .upsert_batch(
                    EmbeddingChannel::Node,
                    7,
                    &[EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 4,
                            item_id,
                        },
                        values,
                    }],
                )
                .unwrap();
        }

        assert_eq!(registry.len(), 2);
        let hits = registry
            .score_candidates(EmbeddingChannel::Node, 4, 7, &diagonal, &[3])
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.item_id, 3);
    }

    #[test]
    fn entry_budget_is_shared_across_embedding_spaces() {
        let mut registry = EmbeddingIndexRegistry::new(3);
        let vector = [1.0, 0.0];
        for (channel, model_hash, item_id) in [
            (EmbeddingChannel::Node, 1, 1),
            (EmbeddingChannel::Summary, 1, 2),
            (EmbeddingChannel::Node, 2, 3),
            (EmbeddingChannel::Summary, 2, 4),
        ] {
            registry
                .upsert_batch(
                    channel,
                    model_hash,
                    &[EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 5,
                            item_id,
                        },
                        values: &vector,
                    }],
                )
                .unwrap();
        }

        assert_eq!(registry.len(), 3);
        assert_eq!(
            registry
                .score_candidates(EmbeddingChannel::Summary, 5, 2, &vector, &[4])
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn sparse_model_spaces_are_bounded_independently_of_rows() {
        let mut registry = EmbeddingIndexRegistry::with_limits(16, 2);
        let vector = [1.0, 0.0];
        for model_hash in 1..=3 {
            registry
                .upsert_batch(
                    EmbeddingChannel::Node,
                    model_hash,
                    &[EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 6,
                            item_id: model_hash,
                        },
                        values: &vector,
                    }],
                )
                .unwrap();
        }

        assert_eq!(registry.spaces.len(), 2);
        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry
                .score_candidates(EmbeddingChannel::Node, 6, 3, &vector, &[3])
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn wide_vectors_recycle_rows_at_the_global_vector_budget() {
        let mut registry = EmbeddingIndexRegistry::new(16);
        registry.max_vector_bytes = 18;
        let vectors = [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [-1.0, 1.0]];
        for (index, vector) in vectors.iter().enumerate() {
            registry
                .upsert_batch(
                    EmbeddingChannel::Node,
                    7,
                    &[EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 4,
                            item_id: index as u64 + 1,
                        },
                        values: vector,
                    }],
                )
                .unwrap();
        }

        assert_eq!(registry.len(), 3);
        assert_eq!(registry.vector_bytes(), 18);
        let hits = registry
            .score_candidates(EmbeddingChannel::Node, 4, 7, &vectors[3], &[4])
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.item_id, 4);
    }

    #[test]
    fn rejected_batches_do_not_evict_valid_rows() {
        let mut registry = EmbeddingIndexRegistry::new(1);
        let valid = [1.0, 0.0];
        registry
            .upsert_batch(
                EmbeddingChannel::Node,
                1,
                &[EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        item_id: 1,
                    },
                    values: &valid,
                }],
            )
            .unwrap();

        let zero = [0.0, 0.0];
        assert!(matches!(
            registry.upsert_batch(
                EmbeddingChannel::Node,
                2,
                &[EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        item_id: 2,
                    },
                    values: &zero,
                }],
            ),
            Err(EmbeddingIndexError::ZeroNorm { .. })
        ));
        assert!(matches!(
            registry.upsert_batch(
                EmbeddingChannel::Node,
                2,
                &[
                    EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 7,
                            item_id: 2,
                        },
                        values: &valid,
                    },
                    EmbeddingVector {
                        key: EmbeddingKey {
                            tenant_id: 7,
                            item_id: 3,
                        },
                        values: &valid,
                    },
                ],
            ),
            Err(EmbeddingIndexError::CapacityExceeded { .. })
        ));
        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry
                .score_candidates(EmbeddingChannel::Node, 7, 1, &valid, &[1])
                .unwrap()
                .len(),
            1
        );
    }
}
