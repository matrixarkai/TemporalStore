// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;

use super::{
    EmbeddingHit, EmbeddingIndex, EmbeddingIndexConfig, EmbeddingIndexError, EmbeddingKey,
    EmbeddingVector, EmbeddingWriteReport,
};

const DEFAULT_MAX_ENTRIES_PER_SPACE: usize = 1_000_000;

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
#[derive(Debug)]
pub struct EmbeddingIndexRegistry {
    max_entries_per_space: usize,
    spaces: HashMap<EmbeddingSpace, EmbeddingIndex>,
}

impl Default for EmbeddingIndexRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES_PER_SPACE)
    }
}

impl EmbeddingIndexRegistry {
    pub fn new(max_entries_per_space: usize) -> Self {
        Self {
            max_entries_per_space: max_entries_per_space.max(1),
            spaces: HashMap::new(),
        }
    }

    pub fn upsert_missing_batch(
        &mut self,
        channel: EmbeddingChannel,
        model_hash: u64,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<Option<EmbeddingWriteReport>, EmbeddingIndexError> {
        let Some(first) = batch.first() else {
            return Ok(None);
        };
        let space = EmbeddingSpace {
            channel,
            model_hash,
            dimensions: first.values.len(),
        };
        let index = self.spaces.entry(space).or_insert(EmbeddingIndex::new(
            EmbeddingIndexConfig::new(space.dimensions, self.max_entries_per_space)
                .with_model_hash(model_hash),
        )?);
        let missing = batch
            .iter()
            .copied()
            .filter(|row| !index.contains(row.key))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(None);
        }
        Ok(Some(index.upsert_batch(&missing)?))
    }

    pub fn upsert_batch(
        &mut self,
        channel: EmbeddingChannel,
        model_hash: u64,
        batch: &[EmbeddingVector<'_>],
    ) -> Result<Option<EmbeddingWriteReport>, EmbeddingIndexError> {
        let Some(first) = batch.first() else {
            return Ok(None);
        };
        let space = EmbeddingSpace {
            channel,
            model_hash,
            dimensions: first.values.len(),
        };
        for (existing_space, index) in &mut self.spaces {
            if existing_space.channel == channel && *existing_space != space {
                for row in batch {
                    index.remove(row.key);
                }
            }
        }
        let index = self.spaces.entry(space).or_insert(EmbeddingIndex::new(
            EmbeddingIndexConfig::new(space.dimensions, self.max_entries_per_space)
                .with_model_hash(model_hash),
        )?);
        Ok(Some(index.upsert_batch(batch)?))
    }

    pub fn score_candidates(
        &self,
        channel: EmbeddingChannel,
        tenant_id: u64,
        model_hash: u64,
        query: &[f32],
        candidate_item_ids: &[u64],
    ) -> Result<Vec<EmbeddingHit>, EmbeddingIndexError> {
        let mut hits = Vec::new();
        let mut positions = HashMap::new();
        for (space, index) in &self.spaces {
            if space.channel != channel
                || space.dimensions != query.len()
                || (model_hash != 0 && space.model_hash != 0 && space.model_hash != model_hash)
            {
                continue;
            }
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

    pub fn len(&self) -> usize {
        self.spaces.values().map(EmbeddingIndex::len).sum()
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
            .upsert_batch(EmbeddingChannel::Node, 2, &[row(&y)])
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
}
