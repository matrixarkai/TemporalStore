// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::atomic::AtomicU64;

pub(super) const FEATURE_ADD_HARD_MAX_SIZE: usize = 100_000;
pub(super) const FEATURE_PAGE_MAGIC: &[u8] = b"TSFPG1\n";
#[cfg(test)]
pub(super) const TIMESTAMPED_KV_PAGE_TARGET_BYTES: usize =
    crate::storage_config::DEFAULT_CONTEXT_PAGE_TARGET_BYTES;
pub(super) const CONTEXT_TIMELINE_FANOUT: u64 = 1024 * 1024;
pub(super) const CONTEXT_DEFAULT_LIMIT: usize = 100;
pub(super) const CONTEXT_MAX_LIMIT: usize = 1000;
pub(super) const CONTEXT_MAX_FILTER_VALUES: usize = 32;
pub(super) const CONTEXT_MAX_RELATED_NODE_HASHES: usize = 64;
pub(super) const CONTEXT_MAX_AUDIT_REFS: usize = 512;
pub(super) const CONTEXT_MAX_PROPAGATE_DEPTH: u32 = 8;
pub(super) const CONTEXT_MAX_INDEX_NAME_BYTES: usize = 64;
pub(super) const CONTEXT_MAX_CANONICAL_NAME_BYTES: usize = 512;
pub(super) const CONTEXT_MAX_ENTITY_NAME_BYTES: usize = 512;
pub(super) const CONTEXT_MAX_ENTITY_VALUE_BYTES: usize = 4096;
pub(super) const CONTEXT_MAX_EMBEDDING_DIM: usize = 4096;
pub(super) const CONTEXT_MAX_SUMMARY_BYTES: usize = 16 * 1024;
pub(super) const CONTEXT_MAX_COMPRESSION_SNIPPET_BYTES: usize = 256;
pub(super) const CONTEXT_MAX_TRAVERSAL_DEPTH: u32 = 16;
pub(super) const CONTEXT_DEFAULT_TRAVERSAL_TOP_K: usize = 8;
pub(super) const CONTEXT_DEFAULT_TRAVERSAL_CANDIDATES: usize = 64;
pub(super) const CONTEXT_MAX_L0_BYTES: usize = 2048;
pub(super) const CONTEXT_MAX_REF_BYTES: usize = 4096;
pub(super) const CONTEXT_MAX_EVENT_TEXT_BYTES: usize = 64 * 1024;
pub(super) const CONTEXT_MAX_COMPACT_ATTRS_BYTES: usize = 8 * 1024;
pub(super) const CONTEXT_NODE_FIELD: &str = "meta";
/// Sentinel slab id for a block that lives in the log rather than a slab file, as the live
/// write path mints it: the address's `offset` is a counter, not a position.
///
/// `pub(crate)` so [`crate::wal_record::is_wal_resident`] can answer for both sentinels in one
/// place rather than each site comparing by hand.
pub(crate) const HOT_PAGE_SLAB_ID: u64 = u64::MAX;
pub(super) static HOT_PAGE_OFFSET: AtomicU64 = AtomicU64::new(1);
