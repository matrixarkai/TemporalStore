// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serving a block back out of the WAL record that holds it.
//!
//! # The hole this closes
//!
//! Under `async_storage` a written value is never handed to the block store. Its only durable
//! copy is its WAL record, and it is served from the memory cache at a synthetic address that
//! names no file. So when the cache drops that entry before a dump materializes it, the read
//! finds nothing: an acked write reads back as MISSING until a full reload replays the WAL.
//! [`super::hot_page_spill`] works around that by copying evicted values to a real slab, which
//! helps only if the spill happened and succeeded.
//!
//! The value was in the WAL the whole time. What was missing was a way to say *where*: the
//! synthetic address is a counter, not a position, so nothing could find the record again.
//!
//! # How the address is resolved
//!
//! An append now reports the log id its record landed at, and a log id survives reclaim -- the
//! record moves when the log is compacted, but the id keeps naming it. So a write registers the
//! synthetic offset it minted against the log id of the record carrying it, and a read that
//! misses the cache resolves through that registration, reads the record, and serves the value
//! from it.
//!
//! # Why identity, and why only single-value commands
//!
//! A registration is keyed by the object id the write derived, which the stored address already
//! carries -- not by when the write happened. Keying on timing (the span of the synthetic
//! counter across one command) would be exact only while nothing else was writing, and would
//! quietly stop registering under concurrency. Matching a stored page back to its record by
//! identity is also what the established design does at commit.
//!
//! Only a command whose bytes ARE the stored value registers at all. A page that is derived
//! state (a serialized series, say) cannot be rebuilt from the record this way, so it is
//! deliberately not attempted and falls through to the existing behaviour unchanged.
//!
//! # Lifetime
//!
//! The registry is live-path state, like the spill redirects: on reload the WAL is replayed and
//! every value is re-derived, so it is never persisted, and a shard's entries are dropped when
//! the shard unloads.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::types::{Command, ShardId};
use crate::wal::{decode_wal_line, LocalWriteAheadLogStore};

/// TS_BLOCK_IN_WAL: serve a cache-missed hot value by reading the WAL record that holds it.
///
/// Default OFF. It changes where a read gets its bytes, so it wants deliberate enabling even
/// though it can only turn a MISSING into a hit -- the fallback path is untouched.
pub(super) fn enabled() -> bool {
    matches!(
        std::env::var("TS_BLOCK_IN_WAL")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// (shard, object id) -> the log holding that value, and where in it.
///
/// The log handle is stored per entry rather than once per process: two engines in one process
/// have separate logs, and a single shared handle would resolve one engine's addresses against
/// the other's log.
type Registration = (LocalWriteAheadLogStore, u64);

fn registry() -> &'static Mutex<HashMap<(ShardId, u64), Registration>> {
    static REGISTRY: OnceLock<Mutex<HashMap<(ShardId, u64), Registration>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The object id this command's value is stored under, if the record can serve it back.
///
/// Recomputed from the command with the same derivation the write path used, so the two agree
/// without the write path having to report it.
pub(super) fn object_id_for(shard_id: ShardId, command: &Command) -> Option<u64> {
    match command {
        Command::StringSet { key, .. } => {
            Some(super::stable_page_object_id(shard_id, "string", key, None))
        }
        _ => None,
    }
}

/// Note that the value for `object_id` lives in the record at `log_id`.
///
/// A later write of the same object replaces the entry, so the registration always names the
/// record holding the current value rather than a superseded one.
pub(super) fn register(
    shard_id: ShardId,
    object_id: u64,
    log_id: u64,
    store: &LocalWriteAheadLogStore,
) {
    if let Ok(mut map) = registry().lock() {
        map.insert((shard_id, object_id), (store.clone(), log_id));
    }
}

/// Forget a shard's registrations. Called when the shard unloads; a reload replays the WAL and
/// re-registers whatever it re-derives.
pub(super) fn clear_shard(shard_id: ShardId) {
    if let Ok(mut map) = registry().lock() {
        map.retain(|(shard, _), _| *shard != shard_id);
    }
}

/// Read the value for `object_id` back out of its WAL record.
///
/// `None` means the object was never registered, its record has been reclaimed, or the record
/// does not carry the value directly -- in every case the caller falls through to the behaviour
/// it had before, so this can only turn a miss into a hit.
pub(super) fn read_value(shard_id: ShardId, object_id: u64) -> Option<Vec<u8>> {
    let (store, log_id) = registry().lock().ok()?.get(&(shard_id, object_id))?.clone();
    // Ask for an upper bound; the read clamps to what the file holds.
    let bytes = store.read_at_log_id(shard_id, log_id, 1 << 20).ok()??;
    let line = bytes.split(|byte| *byte == 10u8).next()?;
    let record = decode_wal_line(line).ok()?;
    match record.command {
        Command::StringSet { value, .. } => Some(value),
        _ => None,
    }
}
