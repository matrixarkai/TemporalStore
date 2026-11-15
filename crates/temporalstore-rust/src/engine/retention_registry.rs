// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Readers that hold block slabs, registered with the engine for as long as they need them.
//!
//! A reader outside the storage manager -- a follower replaying a shared store, a snapshot being
//! streamed -- needs the slabs from some position up to stay readable. Nothing else tells the
//! collector, so the engine keeps the registrations itself and the dependency plan reads them:
//! one source, consulted by the scheduled cycle, the periodic loop and the purge alike.
//!
//! Every registration is a LEASE with an expiry. Without one, a reader that crashed or wedged
//! would hold its slabs for ever and reclamation would stop; with one, the cost of a dead reader
//! is bounded by the lease length. A live reader renews by advancing.
use super::*;
use crate::engine::reports::StorageBlockGcReplayCursor;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
struct Registration {
    retain_from_block_slab_id: u64,
    expires_unix_ms: u64,
    ttl_ms: u64,
    reason: String,
}

/// The registrations of every reader on one engine, keyed by shard and reader id.
#[derive(Debug, Default)]
pub(super) struct RetentionRegistry {
    readers: Mutex<BTreeMap<(ShardId, String), Registration>>,
}

impl RetentionRegistry {
    fn register(
        &self,
        shard_id: ShardId,
        reader_id: &str,
        retain_from_block_slab_id: u64,
        ttl: Duration,
        reason: &str,
        now_unix_ms: u64,
    ) {
        let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
        self.readers
            .lock()
            .expect("retention registry lock poisoned")
            .insert(
                (shard_id, reader_id.to_string()),
                Registration {
                    retain_from_block_slab_id,
                    expires_unix_ms: now_unix_ms.saturating_add(ttl_ms),
                    ttl_ms,
                    reason: reason.to_string(),
                },
            );
    }

    /// Move a live registration to `retain_from_block_slab_id` and renew its lease. A lapsed
    /// registration is not revived: its slabs may already be gone, so the reader is told.
    fn advance(
        &self,
        shard_id: ShardId,
        reader_id: &str,
        retain_from_block_slab_id: u64,
        now_unix_ms: u64,
    ) -> bool {
        let mut readers = self
            .readers
            .lock()
            .expect("retention registry lock poisoned");
        match readers.get_mut(&(shard_id, reader_id.to_string())) {
            Some(entry) if entry.expires_unix_ms > now_unix_ms => {
                entry.retain_from_block_slab_id = retain_from_block_slab_id;
                entry.expires_unix_ms = now_unix_ms.saturating_add(entry.ttl_ms);
                true
            }
            _ => false,
        }
    }

    fn release(&self, shard_id: ShardId, reader_id: &str) {
        self.readers
            .lock()
            .expect("retention registry lock poisoned")
            .remove(&(shard_id, reader_id.to_string()));
    }

    /// The unexpired registrations for one shard, as the cursors the dependency plan consumes.
    /// Lapsed ones are dropped here, so the map cannot grow with dead readers.
    pub(super) fn live_cursors(
        &self,
        shard_id: ShardId,
        now_unix_ms: u64,
    ) -> Vec<StorageBlockGcReplayCursor> {
        let mut readers = self
            .readers
            .lock()
            .expect("retention registry lock poisoned");
        readers.retain(|_, entry| entry.expires_unix_ms > now_unix_ms);
        readers
            .iter()
            .filter(|((shard, _), _)| *shard == shard_id)
            .map(|((shard, reader_id), entry)| StorageBlockGcReplayCursor {
                cursor_id: reader_id.clone(),
                shard_id: *shard,
                retain_from_block_slab_id: entry.retain_from_block_slab_id,
                reason: entry.reason.clone(),
            })
            .collect()
    }
}

/// A registered reader. Dropping it releases the registration.
#[derive(Debug)]
pub struct ReplayReaderLease {
    registry: Arc<RetentionRegistry>,
    shard_id: ShardId,
    reader_id: String,
}

impl ReplayReaderLease {
    /// Report progress: slabs below `retain_from_block_slab_id` are no longer needed, and the
    /// lease is renewed for another full term. Returns false when the lease had already lapsed;
    /// the reader must then treat what it was holding as possibly collected.
    pub fn advance(&self, retain_from_block_slab_id: u64) -> bool {
        self.registry.advance(
            self.shard_id,
            &self.reader_id,
            retain_from_block_slab_id,
            now_unix_ms(),
        )
    }
}

impl Drop for ReplayReaderLease {
    fn drop(&mut self) {
        self.registry.release(self.shard_id, &self.reader_id);
    }
}

impl TemporalEngine {
    /// Hold the block slabs from `retain_from_block_slab_id` up for a reader of `shard_id`.
    ///
    /// The collector neither sets aside nor destroys a slab at or above that id while the lease
    /// lives. The lease lapses `ttl` after the last registration or `advance`.
    pub fn register_replay_reader(
        &self,
        shard_id: ShardId,
        reader_id: &str,
        retain_from_block_slab_id: u64,
        ttl: Duration,
    ) -> ReplayReaderLease {
        self.retention_registry.register(
            shard_id,
            reader_id,
            retain_from_block_slab_id,
            ttl,
            "a registered replay reader still needs this block segment",
            now_unix_ms(),
        );
        ReplayReaderLease {
            registry: Arc::clone(&self.retention_registry),
            shard_id,
            reader_id: reader_id.to_string(),
        }
    }

    /// Cursors of the unexpired registered readers of `shard_id`.
    pub(super) fn registered_replay_cursors(
        &self,
        shard_id: ShardId,
    ) -> Vec<StorageBlockGcReplayCursor> {
        self.retention_registry
            .live_cursors(shard_id, now_unix_ms())
    }

    /// Whether any registered reader of `shard_id` is unexpired.
    pub(crate) fn has_registered_replay_readers(&self, shard_id: ShardId) -> bool {
        !self.registered_replay_cursors(shard_id).is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lapsed_registration_stops_holding_and_cannot_be_revived() {
        let registry = RetentionRegistry::default();
        registry.register(1, "r", 7, Duration::from_millis(100), "", 1_000);
        assert_eq!(
            registry.live_cursors(1, 1_099).len(),
            1,
            "held inside its term"
        );
        assert!(
            registry.live_cursors(1, 1_100).is_empty(),
            "held past its term"
        );
        assert!(
            !registry.advance(1, "r", 9, 1_100),
            "a lapsed lease was revived"
        );
    }

    #[test]
    fn advancing_renews_the_term_and_moves_the_position() {
        let registry = RetentionRegistry::default();
        registry.register(1, "r", 7, Duration::from_millis(100), "", 1_000);
        assert!(registry.advance(1, "r", 12, 1_090));
        let held = registry.live_cursors(1, 1_150);
        assert_eq!(held.len(), 1, "the renewed lease lapsed on the old term");
        assert_eq!(held[0].retain_from_block_slab_id, 12);
    }

    #[test]
    fn a_reader_of_one_shard_does_not_hold_another_shards_slabs() {
        let registry = RetentionRegistry::default();
        registry.register(1, "r", 0, Duration::from_secs(60), "", 0);
        assert!(registry.live_cursors(2, 1).is_empty());
        registry.release(1, "r");
        assert!(registry.live_cursors(1, 1).is_empty());
    }
}
