// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;

use matrixcache::{CacheKey, MultiLayerCache};

use super::constants::HOT_PAGE_SLAB_ID;
use super::page_reads::page_address_cache_key;
use super::records::storage_model_kinds;
use crate::block_store::BlockAddress;
use crate::types::ShardId;

pub(super) type PagePhysicalIdentityKey = (
    u64,
    u64,
    u64,
    Option<u64>,
    Option<u64>,
    Option<u32>,
    Option<u64>,
);

pub(super) fn page_physical_identity_key(address: &BlockAddress) -> PagePhysicalIdentityKey {
    (
        address.page_slab_id,
        address.offset,
        address.length,
        address.page_id,
        address.object_id,
        address.routing_bucket,
        address.generation,
    )
}

pub(super) fn page_memory_resident(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    address: &BlockAddress,
) -> bool {
    cache.peek(&page_address_cache_key(shard_id, address))
}

pub(super) fn page_address_is_memory_only(address: &BlockAddress) -> bool {
    address.page_slab_id == HOT_PAGE_SLAB_ID
}

pub(super) fn invalidate_cache_key(cache: &MultiLayerCache, key: CacheKey, memory_only: bool) {
    if memory_only {
        cache.invalidate_memory_only(&key);
    } else {
        let _ = cache.invalidate(&key);
    }
}

pub(super) fn invalidate_page_addresses(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    addresses: Vec<BlockAddress>,
) {
    invalidate_page_addresses_except(cache, shard_id, addresses, BTreeSet::new());
}

pub(super) fn invalidate_page_addresses_except(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    addresses: Vec<BlockAddress>,
    live_address_keys: BTreeSet<PagePhysicalIdentityKey>,
) {
    if addresses.is_empty() {
        return;
    }
    let keys = addresses
        .into_iter()
        .filter(|address| !live_address_keys.contains(&page_physical_identity_key(address)))
        .map(|address| page_address_cache_key(shard_id, &address))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return;
    }
    let _ = cache.invalidate_batch(&keys);
}

pub(super) fn invalidate_record_all(cache: &MultiLayerCache, shard_id: ShardId, key: &str) {
    let _ = cache.invalidate(&CacheKey::string(shard_id, key));
    for namespace in storage_model_kinds() {
        let _ = cache.invalidate_record(shard_id, namespace, key);
    }
}
