// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(crate) struct PortableCheckpointImage {
    pub(crate) index_bytes: Vec<u8>,
    pub(crate) block_slab_ids: BTreeSet<u64>,
    pub(crate) routing_range: routing_range_stamp::RoutingRangeStamp,
}

impl TemporalEngine {
    pub(crate) fn export_portable_checkpoint_index(
        &self,
        shard_id: ShardId,
    ) -> Result<PortableCheckpointImage, std::io::Error> {
        self.materialize_synthetic_blocks(shard_id);
        let mut previous_pending = None;
        loop {
            let index_bytes = self.export_index_bytes(shard_id)?;
            let snapshot = decode_index_bytes(&index_bytes)
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
            let mut block_slab_ids = BTreeSet::new();
            let mut pending = BTreeSet::new();
            let mut largest = 0;
            let mut observe = |address: &ElementEntry| {
                block_slab_ids.insert(address.block_slab_id());
                if crate::wal_record::is_wal_resident(address.block_slab_id()) {
                    pending.insert((address.block_slab_id(), address.offset(), address.length()));
                    largest = largest.max(address.length());
                }
            };
            // Model maps also cover released buckets and legacy images. Bucket entries
            // additionally retain removal blocks which no live model element names.
            for entry in storage_bucket_internals::collect_model_live_block_entries(&snapshot) {
                observe(&entry.address);
            }
            for bucket in snapshot.bucket_index.bucket_map.values() {
                for block in bucket.block_index.values() {
                    observe(&block.address);
                }
            }
            let (start_routing_bucket, end_routing_bucket) = snapshot.routing_range();
            if pending.is_empty() {
                return Ok(PortableCheckpointImage {
                    index_bytes,
                    block_slab_ids,
                    routing_range: routing_range_stamp::RoutingRangeStamp {
                        start_routing_bucket,
                        end_routing_bucket,
                    },
                });
            }
            if previous_pending.is_some_and(|count| pending.len() >= count) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "portable checkpoint relocation made no progress; retry after writes settle",
                ));
            }
            previous_pending = Some(pending.len());
            let drain = block_slab_ids
                .into_iter()
                .filter(|id| crate::wal_record::is_wal_resident(*id))
                .collect();
            drop(snapshot);
            // Reuse the model-aware rewrite, including packed-member ownership and its
            // durability barrier. One oversized block must not stall every later round.
            self.compact_shard_blocks_relocating(
                shard_id,
                COMPACTION_ROUND_BYTES.max(largest),
                COMPACTION_ROUND_BLOCK_REFS,
                Some(drain),
            )
            .map_err(|err| std::io::Error::other(err.message))?;
        }
    }
}
