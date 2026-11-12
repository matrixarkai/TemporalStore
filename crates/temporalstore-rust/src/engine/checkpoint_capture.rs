// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct DeferredTree<K, V>(Vec<(K, V)>);

impl<K: Clone + Ord, V: Clone> DeferredTree<K, V> {
    fn capture(tree: &BTreeMap<K, V>) -> Self {
        Self(
            tree.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    }
}

impl<K: Ord, V> DeferredTree<K, V> {
    fn finish(self) -> BTreeMap<K, V> {
        self.0.into_iter().collect()
    }
}

struct CapturedBuckets {
    nodes: DeferredTree<u32, BucketNode>,
    dirty: Option<Vec<u32>>,
}

impl CapturedBuckets {
    fn capture(map: &BucketMap) -> Self {
        Self {
            nodes: DeferredTree::capture(&map.nodes),
            dirty: map.dirty.get().map(|set| set.iter().copied().collect()),
        }
    }

    fn finish(self) -> BucketMap {
        let dirty = std::sync::OnceLock::new();
        if let Some(keys) = self.dirty {
            let _ = dirty.set(keys.into_iter().collect());
        }
        BucketMap {
            nodes: self.nodes.finish(),
            dirty,
            // Match BucketMap::clone: copied buffers can have different capacity.
            resident_block_heap: std::sync::OnceLock::new(),
        }
    }
}

enum CapturedDirtyKeys {
    Empty,
    One(Arc<str>),
    Many(Vec<Arc<str>>),
}

struct CapturedDirty {
    by_key: DeferredTree<Arc<str>, u32>,
    by_bucket: Vec<(u32, CapturedDirtyKeys)>,
}

impl CapturedDirty {
    fn capture(index: &DirtyObjectIndex) -> Self {
        Self {
            by_key: DeferredTree::capture(&index.by_key),
            by_bucket: index
                .by_bucket
                .iter()
                .map(|(bucket, keys)| {
                    let keys = match keys {
                        DirtyKeySet::Empty => CapturedDirtyKeys::Empty,
                        DirtyKeySet::One(key) => CapturedDirtyKeys::One(Arc::clone(key)),
                        DirtyKeySet::Many(keys) => {
                            CapturedDirtyKeys::Many(keys.iter().cloned().collect())
                        }
                    };
                    (*bucket, keys)
                })
                .collect(),
        }
    }

    fn finish(self) -> DirtyObjectIndex {
        DirtyObjectIndex {
            by_key: self.by_key.finish(),
            by_bucket: self
                .by_bucket
                .into_iter()
                .map(|(bucket, keys)| {
                    let keys = match keys {
                        CapturedDirtyKeys::Empty => DirtyKeySet::Empty,
                        CapturedDirtyKeys::One(key) => DirtyKeySet::One(key),
                        CapturedDirtyKeys::Many(keys) => {
                            DirtyKeySet::Many(Box::new(keys.into_iter().collect()))
                        }
                    };
                    (bucket, keys)
                })
                .collect(),
        }
    }
}

struct CapturedLookup(Vec<(Arc<str>, DeferredTree<Arc<str>, BlockRefs>)>);

impl CapturedLookup {
    fn capture(lookup: &ObjectBlockLookup) -> Self {
        Self(
            lookup
                .by_model
                .iter()
                .map(|(kind, rows)| (Arc::clone(kind), DeferredTree::capture(rows)))
                .collect(),
        )
    }

    fn finish(self) -> ObjectBlockLookup {
        ObjectBlockLookup {
            by_model: self
                .0
                .into_iter()
                .map(|(kind, rows)| (kind, rows.finish()))
                .collect(),
        }
    }
}

/// An incomplete image is private and cannot be serialized or exposed as a checkpoint.
/// finish() reinstalls all deferred indexes before returning the complete ShardState.
pub(in crate::engine) struct ShardCheckpointCapture {
    rest: ShardState,
    wal: DeferredTree<u64, WalResidentBlock>,
    buckets: CapturedBuckets,
    lookup: CapturedLookup,
    dirty: CapturedDirty,
}

impl ShardState {
    pub(in crate::engine) fn capture_for_checkpoint(&self) -> ShardCheckpointCapture {
        // Exhaustive patterns make any new field a compile error until its copy is defined.
        macro_rules! capture_shard {
            ($($field:ident),* $(,)?) => {{
                let Self { wal_resident_blocks, bucket_index, dirty_objects, $($field),* } = self;
                let (core, buckets, lookup) = capture_core(bucket_index);
                ShardCheckpointCapture {
                    rest: Self {
                        wal_resident_blocks: BTreeMap::new(),
                        bucket_index: core,
                        dirty_objects: DirtyObjectIndex::default(),
                        $($field: $field.clone()),*
                    },
                    wal: DeferredTree::capture(wal_resident_blocks),
                    buckets,
                    lookup,
                    dirty: CapturedDirty::capture(dirty_objects),
                }
            }};
        }
        capture_shard!(
            index_format_version,
            buckets_pending_flag_refresh,
            live_user_bytes,
            live_objects_with_logical_size,
            durable_items,
            expiry_by_deadline,
            side_tables,
            seen,
            buckets,
            object_block_layouts,
            control_state_changes,
            control_state_selection,
            rollups,
            control_coalesce_persist,
            control_state_max_size,
            control_distinct_sketch,
            set_listing_from_map,
            context_event_timeline,
            context_dirty_index,
            applied_wal_sequence,
            wal_apply_discontinuous,
            bucket_recency,
            index_flush_countdown,
            publish_index_flush_countdown,
            promote_scan_done,
            evict_sampler,
            routing_range_start,
            routing_range_end,
            routing_range_known,
            shard_id,
            shard_id_known,
        )
    }
}

fn capture_core(source: &CoreIndex) -> (CoreIndex, CapturedBuckets, CapturedLookup) {
    macro_rules! capture {
        ($($field:ident),* $(,)?) => {{
            let CoreIndex { bucket_map, object_block_lookup, $($field),* } = source;
            (CoreIndex {
                bucket_map: BucketMap::default(),
                object_block_lookup: ObjectBlockLookup::default(),
                $($field: $field.clone()),*
            }, CapturedBuckets::capture(bucket_map), CapturedLookup::capture(object_block_lookup))
        }};
    }
    capture!(
        objects,
        block_slab_live,
        kind_pool,
        object_element_name_block_refs,
        released_buckets,
        hashes,
        sets,
        zsets,
        lists
    )
}

impl ShardCheckpointCapture {
    pub(in crate::engine) fn finish(mut self) -> ShardState {
        self.rest.wal_resident_blocks = self.wal.finish();
        self.rest.bucket_index.bucket_map = self.buckets.finish();
        self.rest.bucket_index.object_block_lookup = self.lookup.finish();
        self.rest.dirty_objects = self.dirty.finish();
        self.rest
    }
}

#[cfg(test)]
#[path = "checkpoint_capture_tests.rs"]
mod tests;
