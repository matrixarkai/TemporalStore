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
//! # THE THREE CASES: ONE STAMP, CURRENT OR REFUSE
//!
//! 1. A STAMP THAT AGREES with the requested range: load, change nothing.
//! 2. A STAMP THAT DISAGREES: **REFUSE, before the decode**, naming both ranges and the file. A
//!    refusal is the only honest answer -- the engine cannot re-file the pages and must not
//!    pretend the range is a filter.
//! 3. NO STAMP AT ALL, over a store that already has on-disk state: **REFUSE**, naming the absent
//!    file. The range such a store was built on is NOT RECOVERABLE. It cannot be read, because
//!    nothing recorded it; and it cannot be inferred, because the evidence does not separate the
//!    candidates -- a store built on the whole keyspace whose keys all happen to fall below 1024
//!    is indistinguishable from one built on `0..1023`, at a probability of about 2.4e-7 for a
//!    single key. An inference that is usually right is a smaller version of the defect this file
//!    exists to remove: it can be wrong SILENTLY, and a store on the wrong range reads perfectly
//!    while sitting outside every per-bucket sweep the shard runs.
//!
//!    THIS ARM USED TO ADOPT THE WHOLE KEYSPACE rather than refuse, reasoning that it "was the
//!    ONLY default a store could have been built on". That is true of the DEFAULT and false of the
//!    CONFIGURATION: `TS_SHARD_END_ROUTING_BUCKET` is documented, and `docs/runtime_tuning.md`
//!    told operators to set it before the first ingest. So a store built narrow before this file
//!    existed was adopted onto the whole keyspace and quietly mis-ranged -- measured at 600 of 600
//!    blocks in `a_pre_stamp_store_built_narrow_is_adopted_onto_a_range_that_cannot_compute_its_buckets`.
//!    The adoption existed to keep existing deployments starting; before the first milestone there
//!    are none to keep, so the honest answer replaces the convenient one.
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

/// A load refused because the stamp names a range the request does not: the range is KNOWN and
/// the configuration disagrees with it.
pub(super) const ROUTING_RANGE_MISMATCH: &str = "routing_range_mismatch";

/// A load refused because the store has state and no stamp: the range is NOT RECOVERABLE.
///
/// A DIFFERENT CODE FROM THE MISMATCH, deliberately. The remedy differs -- there is no range to
/// correct the configuration to -- and the arm this replaces was not a refusal at all, so a caller
/// that used to see a successful load needs to be able to recognise exactly what changed.
pub(super) const ROUTING_RANGE_UNSTAMPED: &str = "routing_range_unstamped";

/// The range a store was built under.
/// `pub(crate)`, not `pub(super)`, because a REPLICATION PAYLOAD carries one. A snapshot image and
/// a shared-store checkpoint both name the range the index they carry was built on, and those live
/// outside `engine`, so the type they name has to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RoutingRangeStamp {
    pub(crate) start_routing_bucket: u32,
    pub(crate) end_routing_bucket: u32,
}

/// Where the stamp lives: beside the base index, named for the shard, so a store copied as a
/// directory carries it.
pub(crate) fn routing_range_stamp_path(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir.join(format!("shard-{shard_id}.routing-range.json"))
}

pub(crate) fn read_routing_range_stamp(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Option<RoutingRangeStamp> {
    let bytes = fs::read(routing_range_stamp_path(index_dir, shard_id)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub(crate) fn write_routing_range_stamp(
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
    /// Load on this range. `write_stamp` says whether the stamp still has to be written.
    ///
    /// There is no "adopted" arm any more. A load either runs on the range it asked for or is
    /// refused, so the requested range is never overridden and there is nothing for the caller to
    /// report about a range it did not choose.
    Load {
        start_routing_bucket: u32,
        end_routing_bucket: u32,
        write_stamp: bool,
    },
    /// Refuse the load. The message names the file that decides and what to do about it.
    ///
    /// TWO CODES, because the two refusals have DIFFERENT REMEDIES and a caller scripting against
    /// this has to tell them apart. A disagreeing stamp means the range is known and the
    /// configuration is wrong -- set it and load again. An absent stamp means the range is not
    /// recoverable at all -- record it if it is known elsewhere, or ingest into a new store. One
    /// code for both would make the second look like a configuration slip.
    Refuse { code: &'static str, message: String },
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
            }
        }
        Some(stamp) => RoutingRangeDecision::Refuse {
            code: ROUTING_RANGE_MISMATCH,
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
        None if store_has_on_disk_state(index_dir, shard_id) => RoutingRangeDecision::Refuse {
            code: ROUTING_RANGE_UNSTAMPED,
            message: format!(
                "shard {shard_id} has stored state but carries no routing-range stamp, so the \
                 routing buckets its blocks are filed under cannot be established. A block's \
                 bucket is the key's hash modulo the RANGE WIDTH, and the width is not recorded \
                 anywhere else in the store, so loading on any range risks filing every later \
                 write in buckets the existing blocks are not in -- readable, and outside the \
                 dump's bucket selection, eviction's victim sampling, the reclaim floor and the \
                 release pass. This is refused rather than guessed at. If the range this store \
                 was built on is KNOWN, record it by writing {} containing \
                 {{\"start_routing_bucket\":<start>,\"end_routing_bucket\":<end>}} and load \
                 again; a store built on the shipped default used \
                 0..{}. If it is not known, ingest into a new store.",
                routing_range_stamp_path(index_dir, shard_id).display(),
                crate::DEFAULT_END_ROUTING_BUCKET,
            ),
        },
        None => RoutingRangeDecision::Load {
            start_routing_bucket: requested_start_routing_bucket,
            end_routing_bucket: requested_end_routing_bucket,
            write_stamp: true,
        },
    }
}
