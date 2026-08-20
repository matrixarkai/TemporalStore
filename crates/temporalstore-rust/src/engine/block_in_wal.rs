// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serving a written page back out of the log record that carries it.
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
//! # Staging
//!
//! A page is often derived state rather than the command's own bytes -- a serialized counter
//! series cannot be rebuilt from the command that bumped it -- so the page itself has to travel
//! with the write. As a write produces pages it puts them aside here; the append attaches
//! whatever was staged to the record it writes and reports the log id that record landed at;
//! and a read resolves the log id and takes its page straight out of the record.
//!
//! The buffer is per thread and cleared at the start of every execute, so a command that stages
//! a page and then fails to append cannot leak it into the next command's record.
//!
//! # Addressing
//!
//! A log id survives reclaim -- the record moves when the log is compacted, but the id keeps
//! naming it -- which is what makes it usable as an address at all. Registrations are keyed on
//! the object id the write derived, which the stored address already carries, so a read finds
//! its record by identity rather than by when the write happened.
//!
//! # Lifetime
//!
//! Registrations are live-path state, like the spill redirects: on reload the WAL is replayed
//! and every page is re-derived, so they are never persisted, and a shard's entries are dropped
//! when the shard unloads.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::types::ShardId;
use crate::wal::{decode_wal_line, LocalWriteAheadLogStore, StagedPage};

/// TS_BLOCK_IN_WAL: stage written pages into their log record and serve them back from it.
///
/// Default OFF. It changes what a record carries and where a read gets its bytes, so it wants
/// deliberate enabling even though it can only turn a MISSING into a hit.
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

thread_local! {
    /// Pages produced by the write currently executing on this thread.
    static STAGED: RefCell<Vec<StagedPage>> = const { RefCell::new(Vec::new()) };
}

/// Start a write with nothing staged.
///
/// Called before every execute. Without it a command that staged a page and then did not append
/// -- a rejected write, a read-only command -- would leave the page for the next command to
/// attach to an unrelated record.
pub(super) fn begin_write() {
    STAGED.with(|staged| staged.borrow_mut().clear());
}

/// Put a page aside for the record this write is about to append.
pub(super) fn stage(object_id: u64, bytes: &[u8]) {
    STAGED.with(|staged| {
        staged.borrow_mut().push(StagedPage {
            object_id,
            bytes: bytes.to_vec(),
        })
    });
}

/// Take what this write staged, leaving nothing behind.
pub(super) fn take_staged() -> Vec<StagedPage> {
    STAGED.with(|staged| std::mem::take(&mut *staged.borrow_mut()))
}

/// (shard, object id) -> the log holding that page, and where in it.
///
/// The log handle is stored per entry rather than once per process: two engines in one process
/// have separate logs, and a single shared handle would resolve one engine's addresses against
/// the other's log -- reading the wrong bytes, silently.
type Registration = (LocalWriteAheadLogStore, u64);

fn registry() -> &'static Mutex<HashMap<(ShardId, u64), Registration>> {
    static REGISTRY: OnceLock<Mutex<HashMap<(ShardId, u64), Registration>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Note that the pages in `record` live in the record at `log_id`.
///
/// A later write of the same object replaces its entry, so a registration always names the
/// record holding the current page rather than a superseded one.
pub(super) fn register_record(
    shard_id: ShardId,
    staged_pages: &[StagedPage],
    log_id: u64,
    store: &LocalWriteAheadLogStore,
) {
    if staged_pages.is_empty() {
        return;
    }
    if let Ok(mut map) = registry().lock() {
        for page in staged_pages {
            map.insert((shard_id, page.object_id), (store.clone(), log_id));
        }
    }
}

/// Forget a shard's registrations. Called when the shard unloads; a reload replays the WAL and
/// re-derives whatever it needs.
pub(super) fn clear_shard(shard_id: ShardId) {
    if let Ok(mut map) = registry().lock() {
        map.retain(|(shard, _), _| *shard != shard_id);
    }
}

/// Read the page for `object_id` back out of the record carrying it.
///
/// `None` means the object was never registered, its record has been reclaimed, or the record
/// does not carry that page -- in every case the caller falls through to the behaviour it had
/// before, so this can only turn a miss into a hit.
pub(super) fn read_page(shard_id: ShardId, object_id: u64) -> Option<Vec<u8>> {
    let (store, log_id) = registry().lock().ok()?.get(&(shard_id, object_id))?.clone();
    // Ask for an upper bound; the read clamps to what the file holds.
    let bytes = store.read_at_log_id(shard_id, log_id, 1 << 20).ok()??;
    let line = bytes.split(|byte| *byte == 10u8).next()?;
    let record = decode_wal_line(line).ok()?;
    record
        .staged_pages
        .into_iter()
        .find(|page| page.object_id == object_id)
        .map(|page| page.bytes)
}
