// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(super) struct CapturedBucket {
    filing: u32,
    position: u32,
    blocks: Vec<(u64, BlockIndex)>,
    tombstones: Vec<(LiveBlockKey, Arc<str>)>,
}

/// Only names, positions and addresses are retained; no object payloads or shard guards.
pub(super) enum CapturedEntries {
    Model(Vec<LiveBlockEntry>),
    Indexed {
        identities: Vec<(Arc<str>, state::BlockRefs)>,
        buckets: Vec<CapturedBucket>,
        released: BTreeSet<u32>,
        range: (u32, u32),
        supplement: Vec<LiveBlockEntry>,
    },
}

impl CapturedEntries {
    #[cfg(test)]
    pub(super) fn input_block_count(&self) -> usize {
        match self {
            Self::Model(entries) => entries.len(),
            Self::Indexed {
                buckets,
                supplement,
                ..
            } => {
                buckets
                    .iter()
                    .map(|bucket| bucket.blocks.len())
                    .sum::<usize>()
                    + supplement.len()
            }
        }
    }

    pub(super) fn capture(
        shard: &ShardState,
        identities: impl Iterator<Item = (Arc<str>, state::BlockRefs)>,
    ) -> Self {
        if shard.bucket_index.bucket_map.is_empty() {
            return Self::Model(collect_model_live_block_entries(shard));
        }
        let buckets = shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(filing, node)| CapturedBucket {
                filing: *filing,
                position: node.routing_bucket,
                blocks: node
                    .block_index
                    .iter()
                    .map(|(handle, block)| (*handle, block.clone()))
                    .collect(),
                tombstones: node
                    .tombstone_elements
                    .iter()
                    .map(|row| (row.block, Arc::clone(&row.object_key)))
                    .collect(),
            })
            .collect();
        let released = shard.bucket_index.released_buckets.clone();
        let supplement = if released.is_empty() {
            Vec::new()
        } else {
            collect_model_live_block_entries(shard)
        };
        Self::Indexed {
            identities: identities.collect(),
            buckets,
            released,
            range: shard.routing_range(),
            supplement,
        }
    }

    #[track_caller]
    pub(super) fn finish(self) -> Vec<LiveBlockEntry> {
        let Self::Indexed {
            identities,
            buckets,
            released,
            range,
            supplement,
        } = self
        else {
            let Self::Model(entries) = self else {
                unreachable!()
            };
            return entries;
        };
        // Preserve source order and last-writer precedence of the existing inversion.
        let mut by_position = HashMap::new();
        let mut by_handle = HashMap::new();
        for (key, refs) in &identities {
            for position in refs.iter() {
                by_position.insert((position.routing_bucket, position.block_ref_key), key);
                by_handle.insert(position.block_ref_key, key);
            }
        }
        let mut entries = Vec::new();
        for bucket in buckets {
            let mut deleted = HashMap::new();
            for (block, key) in &bucket.tombstones {
                deleted.entry(*block).or_insert(key);
            }
            for (handle, block) in bucket.blocks {
                let beside = if block.deleted() {
                    deleted.get(&live_block_key(&block.address)).copied()
                } else {
                    by_position.get(&(bucket.position, handle)).copied()
                }
                .or_else(|| by_handle.get(&handle).copied());
                let Some(key) = beside else {
                    debug_assert!(
                        by_position.is_empty(),
                        "bucket {} holds an unnamed indexed block",
                        bucket.position
                    );
                    continue;
                };
                entries.push(LiveBlockEntry {
                    object_key: Arc::clone(key),
                    kind: block.model_id(),
                    element_name: None,
                    dirty: block.dirty(),
                    deleted: block.deleted(),
                    log_backed: block.log_backed(),
                    address: block.address,
                    filed_routing_bucket: bucket.filing,
                    filing_is_known: true,
                });
            }
        }
        storage_bucket_internals::note_live_block_scan(entries.len());
        for mut entry in supplement {
            let bucket = block_routing_bucket(&entry.object_key, range.0, range.1);
            if released.contains(&bucket) {
                entry.filed_routing_bucket = bucket;
                entry.filing_is_known = true;
                entries.push(entry);
            }
        }
        entries
    }
}
