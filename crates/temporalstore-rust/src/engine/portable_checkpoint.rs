// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::block_store::slab_restore_bindings::SlabRestoreBindings;
use object_record::BlockAddress as ObjectBlockAddress;
use timestamped_series::TimestampedSeries;

pub(crate) struct PortableCheckpointImage {
    pub(crate) index_bytes: Vec<u8>,
    pub(crate) block_slab_ids: BTreeSet<u64>,
    pub(crate) routing_range: routing_range_stamp::RoutingRangeStamp,
}

/// Rewrite the physical locations of a decoded, detached checkpoint image.
/// The output is decoded again on load, which reconstructs the runtime indexes
/// and tallies from the translated on-disk rows.
pub(crate) fn translate_portable_checkpoint_index(
    bytes: &[u8],
    lineage: &str,
    bindings: &SlabRestoreBindings,
) -> std::io::Result<Vec<u8>> {
    let mut snapshot = decode_index_bytes(bytes)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let translate = |entry: &mut ElementEntry| bindings.translate_entry(lineage, entry);
    let index = &mut snapshot.bucket_index;
    for bucket in index.bucket_map.values_mut() {
        for block in bucket.block_index.blocks_mut_unaccounted() {
            translate(&mut block.address)?;
        }
        if !bucket.tombstone_elements.is_empty() {
            let mut rows = bucket
                .tombstone_elements
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            for row in &mut rows {
                row.block.0 = u64::from(bindings.translate_slab_id(lineage, row.block.0)?);
            }
            bucket.rebuild_tombstone_elements(rows);
        }
    }
    index
        .hashes
        .try_visit_element_values_mut(None, |_, mut fields| {
            for (_, address) in fields.iter_mut() {
                translate(address)?;
            }
            Ok::<_, std::io::Error>(())
        })?;
    index
        .sets
        .try_visit_element_values_mut(None, |_, mut members| {
            for (_, address) in members.iter_mut() {
                translate(address)?;
            }
            Ok::<_, std::io::Error>(())
        })?;
    index
        .zsets
        .try_visit_element_values_mut(None, |_, mut members| {
            for (_, (_, address)) in members.iter_mut() {
                translate(address)?;
            }
            Ok::<_, std::io::Error>(())
        })?;
    index
        .lists
        .try_visit_element_values_mut(None, |_, mut elements| {
            for (_, address) in elements.iter_mut() {
                translate(address)?;
            }
            Ok::<_, std::io::Error>(())
        })?;
    for slots in index.objects.values_mut() {
        for slot in slots.values_mut() {
            for (_, address) in slot.points_mut() {
                translate(address)?;
            }
            let record = slot.record_mut();
            if let ObjectBlockAddress::One(word) = record.block_address() {
                if record.block_size() != 0 || word != 0 {
                    let mut address = record.try_address().map_err(std::io::Error::other)?;
                    translate(&mut address)?;
                    record.set_block_address(ObjectBlockAddress::One(address.address_word()));
                }
            }
        }
    }
    for address in snapshot.context_nodes_mut().values_mut() {
        translate(address)?;
    }
    Ok(serialize_index(&snapshot))
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

#[cfg(test)]
mod restore_translation_tests {
    use super::*;
    use crate::block_store::slab_restore_bindings::SourceSlab;

    #[test]
    fn translates_the_persisted_model_and_bucket_addresses_together() {
        let root = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            root.path().join("cache"),
            root.path().join("blocks"),
            root.path().join("indexes"),
        );
        engine.load_shard(1);
        for command in [
            Command::StringSet {
                key: "str".to_owned(),
                value: vec![1; 96],
            },
            Command::HashSet {
                key: "hash".to_owned(),
                field: "field".to_owned(),
                value: vec![2; 96],
            },
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command
                    })
                    .status
                    .ok
            );
        }
        let image = engine.export_portable_checkpoint_index(1).unwrap();
        assert_eq!(image.block_slab_ids.len(), 1);
        let source_id = *image.block_slab_ids.first().unwrap() as u32;
        let mut bindings = SlabRestoreBindings::default();
        assert!(
            translate_portable_checkpoint_index(&image.index_bytes, "source", &bindings).is_err()
        );
        bindings
            .bind(
                SourceSlab {
                    lineage: "source".to_owned(),
                    slab_id: source_id,
                },
                99,
                u32::MAX,
            )
            .unwrap();
        let bytes =
            translate_portable_checkpoint_index(&image.index_bytes, "source", &bindings).unwrap();
        let translated = decode_index_bytes(&bytes).unwrap();
        let live = storage_bucket_internals::collect_model_live_block_entries(&translated);
        assert_eq!(live.len(), 2);
        assert!(live.iter().all(|entry| entry.address.block_slab_id() == 99));
        let indexed = translated
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.block_index.values())
            .collect::<Vec<_>>();
        assert_eq!(indexed.len(), 2);
        assert!(indexed
            .iter()
            .all(|block| block.address.block_slab_id() == 99));
    }
}
