// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn bucket(blocks: usize, live_stride: usize) -> BucketNode {
    let mut bucket = BucketNode::default();
    let mut live = BlockSlabLiveIndex::default();
    let kind = crate::engine::storage_bucket_internals::stored_model_kind("hash");
    let object: Arc<str> = Arc::from("memory-lookup");
    for index in 0..blocks {
        let address = ElementEntry::from_parts(7, index as u64 * 64, 48, Some(index as u64), None);
        let deleted = live_stride == 0 || index % live_stride != 0;
        bucket.insert_block(
            &object,
            BlockIndex::new(kind, address.clone(), false, deleted),
            &mut live,
        );
        bucket.tombstone_elements.push(TombstoneElement {
            object_key: Arc::clone(&object),
            model_id: kind,
            element_name: Arc::from(format!("field-{index}")),
            block: tombstone_block_key(&address),
        });
    }
    bucket.tombstone_elements.push(TombstoneElement {
        object_key: object,
        model_id: kind,
        element_name: Arc::from("missing-field"),
        block: (9, 17, 23),
    });
    bucket
}

fn tree_reconcile(bucket: &mut BucketNode) {
    if bucket.tombstone_elements.is_empty() {
        return;
    }
    let blocks = bucket
        .block_index
        .values()
        .filter(|block| block.deleted())
        .map(|block| tombstone_block_key(&block.address))
        .collect::<std::collections::BTreeSet<_>>();
    bucket
        .tombstone_elements
        .retain(|row| blocks.contains(&row.block));
}

#[test]
fn tombstone_reconcile_preserves_membership_order_and_empty_representation() {
    for size in [0, 1, 128, 4096] {
        for live_stride in [0, 1, 3] {
            let mut compact = bucket(size, live_stride);
            if size > 0 {
                let mut duplicate = compact.tombstone_elements.iter().next().unwrap().clone();
                duplicate.element_name = Arc::from("same-physical-block");
                compact.tombstone_elements.push(duplicate);
            }
            let mut tree = compact.clone();
            tree_reconcile(&mut tree);
            compact.reconcile_tombstone_elements();
            assert_eq!(compact.tombstone_elements, tree.tombstone_elements);
            let first = compact.tombstone_elements.clone();
            compact.reconcile_tombstone_elements();
            assert_eq!(compact.tombstone_elements, first);
            if compact.tombstone_elements.is_empty() {
                assert!(compact.tombstone_elements.0.is_none());
            }
        }
    }
}

#[test]
fn legacy_member_tombstones_preserve_bytes_when_names_are_canonicalized() {
    use crate::engine::container_blocks::{
        decode_container_block, element_name_from_element_key, member_element_name,
        member_from_element_name, tombstone_block, ContainerBlockDecode, ElementKeySpelling,
    };
    for kind in ["set", "zset"] {
        for member in [b"a-member".as_slice(), &[0, 27, 255, 128]] {
            let legacy = hex::encode(member);
            let block = tombstone_block(kind, &legacy).unwrap();
            let ContainerBlockDecode::Framed { items, .. } = decode_container_block(&block) else {
                panic!("legacy member must remain readable");
            };
            assert_eq!(items.len(), 1);
            assert!(items[0].deleted);
            let spelling = ElementKeySpelling::for_kind(kind).unwrap();
            let current = element_name_from_element_key(spelling, &items[0].key).unwrap();
            assert_eq!(current, member_element_name(member));
            assert_eq!(member_from_element_name(&legacy).unwrap(), member);
            assert_eq!(member_from_element_name(&current).unwrap(), member);
        }
    }
}

#[test]
#[cfg(feature = "alloc-probe")]
fn tombstone_reconcile_uses_less_scratch_allocation_at_two_bucket_sizes() {
    for size in [128, 8192] {
        let mut tree = bucket(size, 0);
        let mut compact = tree.clone();
        let probe = crate::alloc_probe::Probe::start();
        tree_reconcile(&mut tree);
        let old = probe.stop();
        let probe = crate::alloc_probe::Probe::start();
        compact.reconcile_tombstone_elements();
        let new = probe.stop();
        assert_eq!(compact.tombstone_elements, tree.tombstone_elements);
        println!(
            "tombstone-reconcile blocks={size} tree_allocs={} compact_allocs={} tree_bytes={} compact_bytes={} tree_chunks={} compact_chunks={}",
            old.allocs, new.allocs, old.alloc_bytes, new.alloc_bytes, old.chunk_bytes, new.chunk_bytes
        );
        assert!(new.allocs < old.allocs);
        assert!(new.alloc_bytes < old.alloc_bytes);
        assert!(new.chunk_bytes < old.chunk_bytes);
    }
}
