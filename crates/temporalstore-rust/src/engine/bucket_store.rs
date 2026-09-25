// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::block_store::{BlockStore, BlockAddress};
use crate::types::ShardId;
use matrixcache::MultiLayerCache;

use super::read_block_bytes;
use super::state::{
    object_component_lookup_key, object_block_lookup_key, ShardState, BucketLayoutState,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketRuntimeState {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub layout: String,
    pub object_ids: Vec<u64>,
    pub block_ref_count: usize,
    pub dirty: bool,
    pub deleted: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub dirty_generation: u64,
    pub last_dump_sequence: u64,
    pub deleted_block_ref_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketStoreRuntimeReport {
    pub bucket_store_runtime_module: bool,
    pub bucket_index_authority: bool,
    pub bucket_count: usize,
    pub block_ref_count: usize,
    pub dirty_bucket_count: usize,
    pub deleted_bucket_count: usize,
    pub empty_buckets: usize,
    pub single_object_buckets: usize,
    pub single_block_object_buckets: usize,
    pub multi_block_object_buckets: usize,
    pub multi_object_buckets: usize,
    pub deleted_block_ref_count: usize,
    pub loading_bucket_count: usize,
    pub in_memory_bucket_count: usize,
    pub ttl_bucket_count: usize,
    pub max_dirty_generation: u64,
    #[serde(rename = "slots")]
    pub buckets: Vec<BucketRuntimeState>,
}

#[allow(dead_code)]
pub(super) fn runtime_report(shard: &ShardState) -> BucketStoreRuntimeReport {
    let mut report = BucketStoreRuntimeReport {
        bucket_store_runtime_module: true,
        bucket_index_authority: !shard.bucket_index.bucket_map.is_empty(),
        bucket_count: shard.bucket_index.bucket_map.len(),
        block_ref_count: 0,
        dirty_bucket_count: 0,
        deleted_bucket_count: 0,
        empty_buckets: 0,
        single_object_buckets: 0,
        single_block_object_buckets: 0,
        multi_block_object_buckets: 0,
        multi_object_buckets: 0,
        deleted_block_ref_count: 0,
        loading_bucket_count: 0,
        in_memory_bucket_count: 0,
        ttl_bucket_count: 0,
        max_dirty_generation: 0,
        buckets: Vec::new(),
    };

    for bucket in shard.bucket_index.bucket_map.values() {
        report.block_ref_count = report.block_ref_count.saturating_add(bucket.block_index.len());
        if bucket.dirty() {
            report.dirty_bucket_count = report.dirty_bucket_count.saturating_add(1);
        }
        if bucket.deleted() {
            report.deleted_bucket_count = report.deleted_bucket_count.saturating_add(1);
        }
        if bucket.loading() {
            report.loading_bucket_count = report.loading_bucket_count.saturating_add(1);
        }
        if bucket.in_memory() {
            report.in_memory_bucket_count = report.in_memory_bucket_count.saturating_add(1);
        }
        if bucket.ttl_ms.is_some() {
            report.ttl_bucket_count = report.ttl_bucket_count.saturating_add(1);
        }
        report.max_dirty_generation = report.max_dirty_generation.max(bucket.dirty_generation);
        let deleted_block_ref_count = bucket.block_index.values().filter(|page| page.deleted).count();
        report.deleted_block_ref_count = report
            .deleted_block_ref_count
            .saturating_add(deleted_block_ref_count);
        match bucket.layout {
            BucketLayoutState::Empty => report.empty_buckets = report.empty_buckets.saturating_add(1),
            BucketLayoutState::SingleObject => {
                report.single_object_buckets = report.single_object_buckets.saturating_add(1)
            }
            BucketLayoutState::SingleBlockObject => {
                report.single_block_object_buckets = report.single_block_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiBlockObject => {
                report.multi_block_object_buckets = report.multi_block_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiObject => {
                report.multi_object_buckets = report.multi_object_buckets.saturating_add(1)
            }
        }
        report.buckets.push(BucketRuntimeState {
            routing_bucket: bucket.routing_bucket,
            layout: bucket_layout_name(bucket.layout).to_string(),
            object_ids: bucket.object_index.iter().copied().collect(),
            block_ref_count: bucket.block_index.len(),
            dirty: bucket.dirty(),
            deleted: bucket.deleted(),
            meta_loaded: bucket.meta_loaded(),
            loading: bucket.loading(),
            in_memory: bucket.in_memory(),
            ttl_ms: bucket.ttl_ms.ms(),
            dirty_generation: bucket.dirty_generation,
            last_dump_sequence: bucket.last_dump_sequence,
            deleted_block_ref_count,
        });
    }

    report
}

#[allow(dead_code)]
fn bucket_layout_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object",
        BucketLayoutState::SingleBlockObject => "single_page_object",
        BucketLayoutState::MultiBlockObject => "multi_page_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

/// Where a read resolves an object's page from.
///
/// THIS IS THE READ PATH, not a reporting path: `read_bucket_index_value` is what
/// `Command::StringGet` calls once the response cache misses. It answers from the bucket index,
/// which is why releasing a bucket's page entries is a question about reads at all -- the fast
/// path in `execute_read_only_fast_path` goes straight to `shard.strings` and never noticed, so a
/// released bucket served every warm read and returned None for every cold one.
///
/// Every "cannot answer" now falls through to the released-bucket lookup rather than returning
/// None, which is what makes a released bucket serve reads identically to a resident one.
pub(super) fn bucket_index_block_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<BlockAddress> {
    if let Some(block_refs) = shard
        .bucket_index
        .block_refs_for(model_id, object_key, component)
    {
        for block_ref in block_refs {
            let Some(bucket) = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket) else {
                continue;
            };
            let Some(page) = bucket.block_index.get(&block_ref.block_ref_key) else {
                continue;
            };
            if !page.deleted
                && page.model_id.as_ref() == model_id
                && &*page.object_key == object_key
                && page.component.as_deref() == component
            {
                return Some(page.address.clone());
            }
        }
        return super::storage_bucket_internals::released_bucket_block_address(
            shard, model_id, object_key, component,
        );
    }

    if !shard.bucket_index.object_block_lookup.is_empty() {
        return super::storage_bucket_internals::released_bucket_block_address(
            shard, model_id, object_key, component,
        );
    }

    shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| {
            !page.deleted
                && page.model_id.as_ref() == model_id
                && &*page.object_key == object_key
                && page.component.as_deref() == component
        })
        .map(|page| page.address.clone())
        .next()
        .or_else(|| {
            super::storage_bucket_internals::released_bucket_block_address(
                shard, model_id, object_key, component,
            )
        })
}

pub(super) fn bucket_index_component_block_addresses(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<(Option<Arc<str>>, BlockAddress)> {
    if let Some(object_refs) = shard.bucket_index.object_block_refs(model_id, object_key) {
        let mut refs = object_refs
            .all_refs()
            .filter_map(|block_ref| {
                let bucket = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket)?;
                let page = bucket.block_index.get(&block_ref.block_ref_key)?;
                if !page.deleted && page.model_id.as_ref() == model_id && &*page.object_key == object_key {
                    Some((page.component.clone(), page.address.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if !refs.is_empty() {
            refs.sort_by(|left, right| left.0.cmp(&right.0));
            return refs;
        }
        return released_component_block_addresses(shard, model_id, object_key);
    }

    if !shard.bucket_index.object_block_lookup.is_empty() {
        return released_component_block_addresses(shard, model_id, object_key);
    }

    let mut refs = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| !page.deleted && page.model_id.as_ref() == model_id && &*page.object_key == object_key)
        .map(|page| (page.component.clone(), page.address.clone()))
        .collect::<Vec<_>>();
    refs.sort_by(|left, right| left.0.cmp(&right.0));
    refs
}

/// The whole-object form of the released-bucket lookup.
///
/// A released kind is component-less by construction (see `released_model_kind_is_addressable`),
/// so "every component of this object" is at most one page and the point lookup answers it. A
/// kind with real components could not be served this way, which is why none is releasable.
fn released_component_block_addresses(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<(Option<Arc<str>>, BlockAddress)> {
    super::storage_bucket_internals::released_bucket_block_address(shard, model_id, object_key, None)
        .map(|address| vec![(None, address)])
        .unwrap_or_default()
}

pub(super) fn read_bucket_index_value(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<Vec<u8>> {
    bucket_index_block_address(shard, model_id, object_key, component)
        .and_then(|address| read_block_bytes(cache, block_store, shard_id, &address))
}
