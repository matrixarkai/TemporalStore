// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE ROUTING RANGE A STORE WAS BUILT UNDER, RECORDED BESIDE ITS INDEX.
//!
//! # WHY THIS FILE EXISTS
//!
//! Routing decides WHERE A KEY'S PAGE IS FILED. A page's bucket is
//! `start + FNV-1a-64(object_key) % (end - start + 1)`, so the range width is the MODULUS and a
//! store written under one range holds its pages under buckets a different range would never
//! compute. Until this file, nothing recorded which range a store was built under: the field on
//! `ShardState` is `#[serde(skip)]` and `Engine::shard_routing_range` falls back to the whole
//! keyspace, so the range lived only in the environment of whichever process happened to load the
//! shard.
//!
//! WHAT THAT COST IS MEASURED, NOT SUPPOSED. A store of 2,000 routed keys written on the whole
//! keyspace and reopened on `0..1023` comes back with **2,000 of 2,000 pages filed in buckets
//! above the shard's own end**. Every record still reads -- which is exactly what makes it quiet --
//! and every one of those pages is outside every per-bucket sweep the shard runs: the dump's bucket
//! selection, eviction's victim sampling, the reclaim floor and the release pass all enumerate
//! against the shard's own range. `docs/runtime_tuning.md` said "Set this before the first ingest"
//! and that sentence was the only thing standing between an operator and that state.
//!
//! Nothing is re-filed on reload, and that is the mechanism: the live write path stamps an explicit
//! bucket onto the address at `append_value`, and `rebuild_bucket_first_index` files a page under
//! its address's own bucket and filters nothing. So a reopened range is consulted only for an
//! address carrying no bucket of its own, and on a populated store there are none.
//!
//! # THE THREE CASES, AND WHY THE MIDDLE ONE IS NOT A REFUSAL
//!
//! 1. A STAMP THAT AGREES with the requested range: load, change nothing.
//! 2. A STAMP THAT DISAGREES: **REFUSE, before the decode**, naming both ranges and the file. This
//!    is the case the paragraph above describes, and a refusal is the only honest answer -- the
//!    engine cannot re-file the pages and must not pretend the range is a filter.
//! 3. NO STAMP AT ALL, over a store that already has on-disk state: the store predates this file.
//!    Its range is not unknown -- the whole keyspace was the ONLY default a store could have been
//!    built on -- so the range it was built under is HONOURED and stamped, and the requested range
//!    is overridden. **This is deliberately not a refusal**: refusing here would stop every
//!    existing deployment from starting, and honouring the built range is what "never silently
//!    mis-route" actually requires. The override is reported through `adopted_legacy` so the caller
//!    can say so out loud rather than doing it quietly.
//!
//! A store with NO stamp and NO on-disk state is new: it is stamped with the requested range, which
//! is what lets the shipped default move for new stores without touching existing ones.
//!
//! # THE STAMP IS NOT PART OF THE INDEX
//!
//! It is its own small file beside `shard-{id}.index.json`, for two reasons. The serialized index
//! shape does not move, so an older build reads a newer store's index unchanged; and the check runs
//! BEFORE the decode, which is where a mismatch has to be caught -- after the decode the pages are
//! already in the wrong buckets in memory.

use super::*;
// Spelled out rather than taken from `super::*`: the engine's glob does not re-export the derive
// macros, and `#[derive(Serialize)]` fails with "cannot find derive macro" rather than with
// anything that points at the glob.
use serde::{Deserialize, Serialize};

/// The end bucket every store built before this file was necessarily built on: the whole keyspace
/// was `startup_load_shard_request`'s and `Engine::load_shard`'s only default.
///
/// NOT DERIVED FROM [`crate::DEFAULT_END_ROUTING_BUCKET`], and it must not be. That constant is what
/// a NEW store gets and is expected to move again; this one is a fact about stores already on disk
/// and is frozen for ever. Tying them together would silently re-range every legacy store the next
/// time the default moved.
pub(super) const LEGACY_END_ROUTING_BUCKET: u32 = u32::MAX;
/// The start bucket those same defaults used. Frozen for the same reason.
pub(super) const LEGACY_START_ROUTING_BUCKET: u32 = 0;

/// The range a store was built under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RoutingRangeStamp {
    pub(super) start_routing_bucket: u32,
    pub(super) end_routing_bucket: u32,
}

/// Where the stamp lives: beside the base index, named for the shard, so a store copied as a
/// directory carries it.
pub(super) fn routing_range_stamp_path(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir.join(format!("shard-{shard_id}.routing-range.json"))
}

pub(super) fn read_routing_range_stamp(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Option<RoutingRangeStamp> {
    let bytes = fs::read(routing_range_stamp_path(index_dir, shard_id)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub(super) fn write_routing_range_stamp(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    stamp: RoutingRangeStamp,
) -> Result<(), std::io::Error> {
    fs::create_dir_all(index_dir)?;
    let bytes = serde_json::to_vec(&stamp)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    fs::write(routing_range_stamp_path(index_dir, shard_id), bytes)
}

/// Whether this shard already has state on disk -- the question that separates a NEW store from one
/// built before the stamp existed.
///
/// TWO PLACES, NOT ONE. The base index file is the obvious one, but a shard whose base has never
/// been materialized can still have durable bucket dump manifests, and a load recovers from those.
/// Asking only about the base index would call such a store new and stamp it with the requested
/// range, which is exactly the silent re-ranging this whole file exists to prevent.
pub(super) fn store_has_on_disk_state(index_dir: &std::path::Path, shard_id: ShardId) -> bool {
    if index_dir
        .join(format!("shard-{shard_id}.index.json"))
        .exists()
    {
        return true;
    }
    let manifest_dir = crate::engine::bucket_dump_io::bucket_dump_manifest_dir(index_dir, shard_id);
    fs::read_dir(manifest_dir)
        .map(|mut entries| entries.any(|entry| entry.is_ok()))
        .unwrap_or(false)
}

/// What a load should do about the range it was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RoutingRangeDecision {
    /// Load on this range. `write_stamp` says whether the stamp still has to be written;
    /// `adopted_legacy` says the requested range was OVERRIDDEN because the store predates the
    /// stamp, which the caller must report rather than do quietly.
    Load {
        start_routing_bucket: u32,
        end_routing_bucket: u32,
        write_stamp: bool,
        adopted_legacy: bool,
    },
    /// Refuse the load. The message names both ranges and the file that decides.
    Refuse { message: String },
}

/// THE DECISION, taken before any decode.
pub(super) fn decide_routing_range(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    requested_start_routing_bucket: u32,
    requested_end_routing_bucket: u32,
) -> RoutingRangeDecision {
    match read_routing_range_stamp(index_dir, shard_id) {
        Some(stamp)
            if stamp.start_routing_bucket == requested_start_routing_bucket
                && stamp.end_routing_bucket == requested_end_routing_bucket =>
        {
            RoutingRangeDecision::Load {
                start_routing_bucket: requested_start_routing_bucket,
                end_routing_bucket: requested_end_routing_bucket,
                write_stamp: false,
                adopted_legacy: false,
            }
        }
        Some(stamp) => RoutingRangeDecision::Refuse {
            message: format!(
                "shard {shard_id} was built on routing buckets {}..{} and is being loaded on \
                 {requested_start_routing_bucket}..{requested_end_routing_bucket}. A page's \
                 bucket is the key's hash modulo the RANGE WIDTH, so the store's pages are filed \
                 under buckets this range does not contain: every record would still read while \
                 sitting outside the dump's bucket selection, eviction's victim sampling, the \
                 reclaim floor and the release pass. Load this store on {}..{} -- set \
                 TS_SHARD_START_ROUTING_BUCKET and TS_SHARD_END_ROUTING_BUCKET -- or ingest into \
                 a new store. The range this store was built on is recorded in {}",
                stamp.start_routing_bucket,
                stamp.end_routing_bucket,
                stamp.start_routing_bucket,
                stamp.end_routing_bucket,
                routing_range_stamp_path(index_dir, shard_id).display(),
            ),
        },
        None if store_has_on_disk_state(index_dir, shard_id) => RoutingRangeDecision::Load {
            start_routing_bucket: LEGACY_START_ROUTING_BUCKET,
            end_routing_bucket: LEGACY_END_ROUTING_BUCKET,
            write_stamp: true,
            adopted_legacy: requested_start_routing_bucket != LEGACY_START_ROUTING_BUCKET
                || requested_end_routing_bucket != LEGACY_END_ROUTING_BUCKET,
        },
        None => RoutingRangeDecision::Load {
            start_routing_bucket: requested_start_routing_bucket,
            end_routing_bucket: requested_end_routing_bucket,
            write_stamp: true,
            adopted_legacy: false,
        },
    }
}
