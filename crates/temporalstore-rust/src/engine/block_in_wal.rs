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
/// Default ON. Without it an acked write reads back as MISSING when its cache entry is dropped
/// before a dump -- a correctness result, not a tuning one -- and the cost that kept it off is
/// gone: a staged page now costs about a third over its contents rather than five times.
///
/// Set to a falsey value to restore the previous behaviour: records carry no pages, and an
/// evicted write is served only if the spill path happened to catch it.
pub(super) fn enabled() -> bool {
    !matches!(
        std::env::var("TS_BLOCK_IN_WAL")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
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
    OUTCOMES.with(|outcomes| outcomes.borrow_mut().clear());
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

thread_local! {
    /// Index outcomes produced by the write currently executing on this thread.
    static OUTCOMES: RefCell<Vec<crate::wal::WalOutcomeItem>> =
        const { RefCell::new(Vec::new()) };
}

/// Put aside what this write DID: which object, and where its page now lives.
///
/// Staged for the same reason pages are -- the record does not exist yet, so there is nowhere
/// to put it until the append.
pub(super) fn stage_outcome(item: crate::wal::WalOutcomeItem) {
    OUTCOMES.with(|outcomes| outcomes.borrow_mut().push(item));
}

/// Take what this write recorded doing, leaving nothing behind.
/// How many outcomes this write has staged so far, without consuming them.
pub(super) fn staged_outcome_count() -> usize {
    OUTCOMES.with(|outcomes| outcomes.borrow().len())
}

pub(super) fn take_outcomes() -> Vec<crate::wal::WalOutcomeItem> {
    OUTCOMES.with(|outcomes| std::mem::take(&mut *outcomes.borrow_mut()))
}

/// Take what this write staged, leaving nothing behind.
pub(super) fn take_staged() -> Vec<StagedPage> {
    STAGED.with(|staged| std::mem::take(&mut *staged.borrow_mut()))
}

/// (shard, object id) -> the log holding that page, where in it, and the WAL sequence of the
/// record carrying it.
///
/// The log handle is stored per entry rather than once per process: two engines in one process
/// have separate logs, and a single shared handle would resolve one engine's addresses against
/// the other's log -- reading the wrong bytes, silently.
///
/// The sequence is what lets WAL reclaim coexist with these registrations: a registered page's
/// only durable copy is its record, so reclaim must never truncate below the lowest registered
/// sequence (see [`min_registered_sequence`]).
type Registration = (LocalWriteAheadLogStore, u64, u64);

/// Keyed by the STORE as well as the shard and object.
///
/// An object id is derived from kind + key, not from who wrote it, and every embedded engine in
/// a process serves shard 1. Keyed on (shard, object) alone, the last engine to write a key owned
/// that key for the whole process and handed its own log to whoever asked next -- so one engine
/// served another engine's bytes for any key they happened to share.
fn registry() -> &'static Mutex<HashMap<(usize, ShardId, u64), Registration>> {
    static REGISTRY: OnceLock<Mutex<HashMap<(usize, ShardId, u64), Registration>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Note that the pages in `record` live in the record at `log_id`, carried by the WAL record
/// at `sequence`.
///
/// A later write of the same object replaces its entry, so a registration always names the
/// record holding the current page rather than a superseded one.
pub(super) fn register_record(
    block_store: &crate::block_store::LocalBlockStore,
    shard_id: ShardId,
    staged_pages: &[StagedPage],
    log_id: u64,
    sequence: u64,
    store: &LocalWriteAheadLogStore,
) {
    if staged_pages.is_empty() {
        return;
    }
    if let Ok(mut map) = registry().lock() {
        for page in staged_pages {
            map.insert(
                (block_store.store_id(), shard_id, page.object_id),
                (store.clone(), log_id, sequence),
            );
        }
    }
}

/// Register a page whose location came from the index rather than from an append.
///
/// The append path learns the log id by writing the record; a reload learns it by reading the
/// index. Same fact, different source, so it lands in the same table -- which is what lets the
/// read path stay exactly as it was.
pub(super) fn register_at(
    block_store: &crate::block_store::LocalBlockStore,
    shard_id: ShardId,
    object_id: u64,
    log_id: u64,
    sequence: u64,
    store: &LocalWriteAheadLogStore,
) {
    if let Ok(mut map) = registry().lock() {
        map.insert(
            (block_store.store_id(), shard_id, object_id),
            (store.clone(), log_id, sequence),
        );
    }
}

/// The lowest WAL sequence any of this shard's registrations IN THIS LOG still depends on, or
/// `None` when nothing is registered. WAL reclaim uses this as the block-retention floor: a
/// record at or above it may hold the only copy of a page the served index still points at, so
/// truncating it would turn an acked write into a MISSING read.
///
/// Filtered by log identity, not just shard id: the registry is process-wide and every
/// embedded engine serves shard 1, so without the filter one engine's registrations would pin
/// every other engine's reclaim floor forever.
pub(super) fn min_registered_sequence(
    block_store: &crate::block_store::LocalBlockStore,
    shard_id: ShardId,
    store: &LocalWriteAheadLogStore,
) -> Option<u64> {
    let map = registry().lock().ok()?;
    map.iter()
        .filter(|((owner, shard, _), (reg_store, _, _))| {
            *owner == block_store.store_id() && *shard == shard_id && reg_store.same_log(store)
        })
        .map(|(_, (_, _, sequence))| *sequence)
        .min()
}

/// How many registrations this shard is holding.
///
/// The registry resolves a synthetic address to the record carrying its bytes, and it is process
/// static: one entry per distinct object, held until the shard unloads. A test needs to see the
/// count to say anything about whether it grows with the log.
pub(super) fn registration_count(
    block_store: &crate::block_store::LocalBlockStore,
    shard_id: ShardId,
) -> usize {
    let Ok(map) = registry().lock() else {
        return 0;
    };
    let owner = block_store.store_id();
    map.keys()
        .filter(|(store_id, shard, _)| *store_id == owner && *shard == shard_id)
        .count()
}

/// Retire one object's registration.
///
/// Called when its page stops being log-resident -- materialised into the block store, so the
/// index now names a real slab. Keeping it would pin the WAL retention floor to a record nothing
/// needs, which is not a leak of bytes but of RECLAIM: the floor is the lowest live registration,
/// so one stale entry holds the whole log.
pub(super) fn deregister(
    block_store: &crate::block_store::LocalBlockStore,
    shard_id: ShardId,
    object_id: u64,
) {
    if let Ok(mut map) = registry().lock() {
        map.remove(&(block_store.store_id(), shard_id, object_id));
    }
}


/// Forget a shard's registrations. Called when the shard unloads; a reload replays the WAL and
/// re-derives whatever it needs.
pub(super) fn clear_shard(block_store: &crate::block_store::LocalBlockStore, shard_id: ShardId) {
    if let Ok(mut map) = registry().lock() {
        let owner = block_store.store_id();
        map.retain(|(store_id, shard, _), _| !(*store_id == owner && *shard == shard_id));
    }
}

/// Read the page for `object_id` back out of the record carrying it.
///
/// `None` means the object was never registered, its record has been reclaimed, or the record
/// does not carry that page -- in every case the caller falls through to the behaviour it had
/// before, so this can only turn a miss into a hit.
pub(super) fn read_page(
    block_store: &crate::block_store::LocalBlockStore,
    shard_id: ShardId,
    object_id: u64,
) -> Option<Vec<u8>> {
    let (store, log_id, _) = registry()
        .lock()
        .ok()?
        .get(&(block_store.store_id(), shard_id, object_id))?
        .clone();
    // One batch record carries many pages, and an ingest reads several of the fields its own
    // batch just wrote -- so the same record used to be pread and re-parsed once per page. WAL
    // records are immutable once written (append-only), so a small decoded-record LRU cannot go
    // stale: a superseding write registers a NEWER log_id and old entries simply age out.
    if let Ok(cache) = record_lru().lock() {
        // Keyed by (log identity, shard, log id) -- a log id is a byte offset within ONE log,
        // so the same number names unrelated records in different engines' logs.
        if let Some((_, _, _, pages)) = cache
            .iter()
            .find(|(s, shard, l, _)| *shard == shard_id && *l == log_id && s.same_log(&store))
        {
            if let Some(page) = pages.iter().find(|page| page.object_id == object_id) {
                return Some(page.bytes.clone());
            }
        }
    }
    // Adaptive pread: most records terminate well inside 128KB, so try that first and escalate
    // only when the record does not end in the chunk -- a fixed 1MB upper bound makes every
    // point read cost a megabyte of I/O.
    //
    // Where the record ends is asked of the FRAME, not guessed from a newline. Looking for one
    // is right only while a record cannot contain the byte it ends with: a length-framed record
    // carries 0x0A freely, so `contains` answers yes on the first payload that holds one and the
    // split then cuts the record in half. The frame reader says "not all here yet" for exactly
    // the case the newline probe was approximating, so escalation reads better than it did.
    let mut record = None;
    for size in [128u64 << 10, 1 << 20, u64::MAX] {
        let bytes = store.read_at_log_id(shard_id, log_id, size).ok()??;
        match crate::log_framing::next_frame(&bytes) {
            Ok(Some((consumed, _))) => {
                record = decode_wal_line(&bytes[..consumed]).ok();
                break;
            }
            // The record declares more bytes than this window holds: read a bigger one.
            Ok(None) => continue,
            Err(_) => return None,
        }
    }
    let record = record?;
    let pages = record.staged_pages;
    if let Ok(mut cache) = record_lru().lock() {
        if cache.len() >= 8 {
            cache.remove(0);
        }
        cache.push((store.clone(), shard_id, log_id, pages.clone()));
    }
    pages
        .into_iter()
        .find(|page| page.object_id == object_id)
        .map(|page| page.bytes)
}

/// Decoded staged pages of recently read records, keyed by (log identity, shard, log id).
/// Tiny on purpose: the working set is "the record(s) the current request's batch just wrote".
/// Entries cannot go stale: WAL records are immutable, a superseding write registers a newer
/// log id, and the post-dump WAL sweep never truncates a registered record (its floor).
fn record_lru() -> &'static Mutex<Vec<(LocalWriteAheadLogStore, ShardId, u64, Vec<StagedPage>)>> {
    static LRU: OnceLock<Mutex<Vec<(LocalWriteAheadLogStore, ShardId, u64, Vec<StagedPage>)>>> =
        OnceLock::new();
    LRU.get_or_init(|| Mutex::new(Vec::new()))
}
