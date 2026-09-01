// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::block_store::{LocalBlockStore, BlockAddress};
use crate::types::ShardId;
use matrixcache::MultiLayerCache;

use super::read_page_bytes;
use super::state::{
    object_component_lookup_key, object_page_lookup_key, ShardState, BucketLayoutState,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketRuntimeState {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub layout: String,
    pub object_ids: Vec<u64>,
    pub page_ref_count: usize,
    pub dirty: bool,
    pub deleted: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub dirty_generation: u64,
    pub last_dump_sequence: u64,
    pub deleted_page_ref_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketStoreRuntimeReport {
    #[serde(rename = "slot_store_runtime_module")]
    pub bucket_store_runtime_module: bool,
    #[serde(rename = "slot_index_authority")]
    pub bucket_index_authority: bool,
    #[serde(rename = "slot_count")]
    pub bucket_count: usize,
    pub page_ref_count: usize,
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: usize,
    #[serde(rename = "deleted_slot_count")]
    pub deleted_bucket_count: usize,
    #[serde(rename = "empty_slots")]
    pub empty_buckets: usize,
    #[serde(rename = "single_object_slots")]
    pub single_object_buckets: usize,
    #[serde(rename = "single_page_object_slots")]
    pub single_page_object_buckets: usize,
    #[serde(rename = "multi_page_object_slots")]
    pub multi_page_object_buckets: usize,
    #[serde(rename = "multi_object_slots")]
    pub multi_object_buckets: usize,
    pub deleted_page_ref_count: usize,
    #[serde(rename = "loading_slot_count")]
    pub loading_bucket_count: usize,
    #[serde(rename = "in_memory_slot_count")]
    pub in_memory_bucket_count: usize,
    #[serde(rename = "ttl_slot_count")]
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
        page_ref_count: 0,
        dirty_bucket_count: 0,
        deleted_bucket_count: 0,
        empty_buckets: 0,
        single_object_buckets: 0,
        single_page_object_buckets: 0,
        multi_page_object_buckets: 0,
        multi_object_buckets: 0,
        deleted_page_ref_count: 0,
        loading_bucket_count: 0,
        in_memory_bucket_count: 0,
        ttl_bucket_count: 0,
        max_dirty_generation: 0,
        buckets: Vec::new(),
    };

    for bucket in shard.bucket_index.bucket_map.values() {
        report.page_ref_count = report.page_ref_count.saturating_add(bucket.page_index.len());
        if bucket.dirty {
            report.dirty_bucket_count = report.dirty_bucket_count.saturating_add(1);
        }
        if bucket.deleted {
            report.deleted_bucket_count = report.deleted_bucket_count.saturating_add(1);
        }
        if bucket.loading {
            report.loading_bucket_count = report.loading_bucket_count.saturating_add(1);
        }
        if bucket.in_memory {
            report.in_memory_bucket_count = report.in_memory_bucket_count.saturating_add(1);
        }
        if bucket.ttl_ms.is_some() {
            report.ttl_bucket_count = report.ttl_bucket_count.saturating_add(1);
        }
        report.max_dirty_generation = report.max_dirty_generation.max(bucket.dirty_generation);
        let deleted_page_ref_count = bucket.page_index.values().filter(|page| page.deleted).count();
        report.deleted_page_ref_count = report
            .deleted_page_ref_count
            .saturating_add(deleted_page_ref_count);
        match bucket.layout {
            BucketLayoutState::Empty => report.empty_buckets = report.empty_buckets.saturating_add(1),
            BucketLayoutState::SingleObject => {
                report.single_object_buckets = report.single_object_buckets.saturating_add(1)
            }
            BucketLayoutState::SinglePageObject => {
                report.single_page_object_buckets = report.single_page_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiPageObject => {
                report.multi_page_object_buckets = report.multi_page_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiObject => {
                report.multi_object_buckets = report.multi_object_buckets.saturating_add(1)
            }
        }
        report.buckets.push(BucketRuntimeState {
            routing_bucket: bucket.routing_bucket,
            layout: bucket_layout_name(bucket.layout).to_string(),
            object_ids: bucket.object_index.iter().copied().collect(),
            page_ref_count: bucket.page_index.len(),
            dirty: bucket.dirty,
            deleted: bucket.deleted,
            meta_loaded: bucket.meta_loaded,
            loading: bucket.loading,
            in_memory: bucket.in_memory,
            ttl_ms: bucket.ttl_ms,
            dirty_generation: bucket.dirty_generation,
            last_dump_sequence: bucket.last_dump_sequence,
            deleted_page_ref_count,
        });
    }

    report
}

#[allow(dead_code)]
fn bucket_layout_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object",
        BucketLayoutState::SinglePageObject => "single_page_object",
        BucketLayoutState::MultiPageObject => "multi_page_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

pub(super) fn bucket_index_page_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<BlockAddress> {
    if let Some(page_refs) = shard
        .bucket_index
        .page_refs_for(model_id, object_key, component)
    {
        for page_ref in page_refs {
            let Some(bucket) = shard.bucket_index.bucket_map.get(&page_ref.routing_bucket) else {
                continue;
            };
            let Some(page) = bucket.page_index.get(&page_ref.page_ref_key) else {
                continue;
            };
            if !page.deleted
                && page.model_id.as_ref() == model_id
                && page.object_key == object_key
                && page.component.as_deref() == component
            {
                return Some(page.address.clone());
            }
        }
        return None;
    }

    if !shard.bucket_index.object_page_lookup.is_empty() {
        return None;
    }

    shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.page_index.values())
        .filter(|page| {
            !page.deleted
                && page.model_id.as_ref() == model_id
                && page.object_key == object_key
                && page.component.as_deref() == component
        })
        .map(|page| page.address.clone())
        .next()
}

pub(super) fn bucket_index_component_page_addresses(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<(Option<Arc<str>>, BlockAddress)> {
    if let Some(object_refs) = shard.bucket_index.object_page_refs(model_id, object_key) {
        let mut refs = object_refs
            .all_refs()
            .filter_map(|page_ref| {
                let bucket = shard.bucket_index.bucket_map.get(&page_ref.routing_bucket)?;
                let page = bucket.page_index.get(&page_ref.page_ref_key)?;
                if !page.deleted && page.model_id.as_ref() == model_id && page.object_key == object_key {
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
        return Vec::new();
    }

    if !shard.bucket_index.object_page_lookup.is_empty() {
        return Vec::new();
    }

    let mut refs = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.page_index.values())
        .filter(|page| !page.deleted && page.model_id.as_ref() == model_id && page.object_key == object_key)
        .map(|page| (page.component.clone(), page.address.clone()))
        .collect::<Vec<_>>();
    refs.sort_by(|left, right| left.0.cmp(&right.0));
    refs
}

pub(super) fn read_bucket_index_value(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<Vec<u8>> {
    bucket_index_page_address(shard, model_id, object_key, component)
        .and_then(|address| read_page_bytes(cache, page_store, shard_id, &address))
}
