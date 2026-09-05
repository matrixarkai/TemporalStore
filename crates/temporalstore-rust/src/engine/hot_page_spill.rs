// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Durable read-by-address fallback for log-backed hot pages.
//!
//! Under `async_storage`, a freshly written value is served only from the in-memory cache tier
//! at a SYNTHETIC page address (`page_slab_id == HOT_PAGE_SLAB_ID`); it is not written to the
//! block store, so it exists on disk only as its WAL record. That has two costs:
//!
//!   1. Correctness -- if the memory-only entry is evicted before the next dump/flush
//!      materializes it, a read misses the cache and `page_store.read(synthetic_address)` finds
//!      no file, so the acked write reads back as MISSING (looks deleted). Only a full reload
//!      (WAL replay) would recover it.
//!   2. Memory footprint -- hot pages are a primary consumer of the DRAM tier because their only
//!      residence is memory.
//!
//! This module fixes both by SPILLING a hot page to a real slab exactly when it is evicted from
//! the cache: an eviction handler writes the evicted bytes to the durable block store and records
//! a redirect from the synthetic address to the real one. Reads consult the redirect on a
//! hot-address cache miss and read the real slab, so an evicted hot page is never read-as-missing.
//! Because the eviction moves the bytes out of DRAM (to the block store / SSD-backed slab) and
//! keeps only a tiny redirect entry, steady-state RSS drops as well.
//!
//! The redirect map is purely live-path state: on crash/reload the WAL is the source of truth and
//! re-derives every hot page from scratch, so the map is never persisted. It is bounded by
//! clearing a shard's entries on unload.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use matrixcache::{CacheEvictionRecord, MultiLayerCache};

use crate::block_store::{BlockAddress, LocalBlockStore};
use crate::types::ShardId;

use super::constants::HOT_PAGE_SLAB_ID;

fn redirects() -> &'static Mutex<HashMap<(ShardId, u64), BlockAddress>> {
    static REDIRECTS: OnceLock<Mutex<HashMap<(ShardId, u64), BlockAddress>>> = OnceLock::new();
    REDIRECTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The synthetic hot-page cache key's record_key, i.e. `segment-{HOT_PAGE_SLAB_ID:020}`.
fn hot_page_record_key() -> String {
    format!("segment-{HOT_PAGE_SLAB_ID:020}")
}

/// Parse `(offset, routing_bucket)` out of a hot-page cache key selector. Selector forms produced
/// for a hot page are `slot-{routing}:{offset}:{length}` or `{offset}:{length}`; the offset is the
/// second-to-last colon-separated component and any leading `slot-{n}` carries the routing bucket.
fn parse_hot_selector(selector: &str) -> Option<(u64, Option<u32>)> {
    let parts: Vec<&str> = selector.split(':').collect();
    if parts.len() < 2 {
        return None;
    }
    let offset: u64 = parts[parts.len() - 2].parse().ok()?;
    let routing_bucket = parts
        .first()
        .and_then(|head| head.strip_prefix("slot-"))
        .and_then(|slot| slot.parse::<u32>().ok());
    Some((offset, routing_bucket))
}

/// Install the eviction handler that spills this engine's evicted hot pages to ITS block store.
///
/// Registered per engine cache (not process-globally): every engine owns its own cache + block
/// store, and a spilled page's real address is only valid within the block store it was written
/// to. A process-wide handler capturing one engine's block store would misroute another engine's
/// spills (their real slab lives in a different store). `register_eviction_callback` replaces the
/// cache's callback, so a cache used 1:1 with an engine ends up with the correct store; the shared
/// redirect map stays coherent because hot-page offsets are globally unique (one process-wide
/// atomic), so an engine only ever looks up offsets it minted itself.
///
/// Always installed. `TS_HOT_PAGE_SPILL` used to be able to skip it -- strictly additive, so the
/// off position only restored an acked-write-reads-as-missing bug -- and the only callers that
/// ever set it were four tests that set it AFTER building their engine, by which time this had
/// already run. `TemporalEngine::disable_hot_page_spill_for_test` is how an engine is told, and
/// it works because `register_eviction_callback` replaces the callback rather than adding one.
pub(super) fn install_spill_handler(cache: &MultiLayerCache, block_store: &LocalBlockStore) {
    let block_store = block_store.clone();
    let hot_record_key = hot_page_record_key();
    cache.register_eviction_callback(move |record: CacheEvictionRecord| {
        spill_evicted_hot_page(&block_store, &hot_record_key, record);
    });
}

fn spill_evicted_hot_page(
    block_store: &LocalBlockStore,
    hot_record_key: &str,
    record: CacheEvictionRecord,
) {
    if record.key.namespace != "page" || record.key.record_key != hot_record_key {
        return;
    }
    let Some((offset, routing_bucket)) = parse_hot_selector(&record.key.selector) else {
        return;
    };
    let shard_id = record.key.shard_id;
    // Already spilled (e.g. an earlier memory eviction, now re-evicted from a lower tier): the
    // existing redirect still points at valid durable bytes, so skip the redundant write.
    if redirects()
        .lock()
        .map(|map| map.contains_key(&(shard_id, offset)))
        .unwrap_or(false)
    {
        return;
    }
    // Write the evicted bytes to a real slab. object_id is not needed for read-by-address
    // (reads resolve on slab+offset+length), so we only preserve the routing bucket.
    match block_store.append_with_page_metadata(&record.value, None, routing_bucket) {
        Ok(real_address) => {
            if let Ok(mut map) = redirects().lock() {
                map.insert((shard_id, offset), real_address);
            }
        }
        Err(err) => {
            // The value is still recoverable from the WAL on reload; log so an operator can see a
            // hot page that failed to spill (it will read-as-missing until reload).
            tracing::error!(
                shard_id,
                offset,
                error = %err,
                "failed to spill evicted hot page to a durable slab"
            );
        }
    }
}

/// Look up the real slab address a spilled hot page was written to, if any. Keyed by the synthetic
/// address's `(shard_id, offset)`.
pub(super) fn lookup_spilled(shard_id: ShardId, offset: u64) -> Option<BlockAddress> {
    redirects()
        .lock()
        .ok()
        .and_then(|map| map.get(&(shard_id, offset)).cloned())
}

/// Drop a shard's redirect entries (called on unload) so the map stays bounded across a shard's
/// load/unload lifecycle.
pub(super) fn clear_shard(shard_id: ShardId) {
    if let Ok(mut map) = redirects().lock() {
        map.retain(|(entry_shard, _), _| *entry_shard != shard_id);
    }
}
