// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW A `context_nodes` ENTRY CAN STILL ARRIVE, AND WHICH DOORS THE FORMAT STAMP GUARDS.
//!
//! # THE QUESTION
//!
//! `ShardState::context_nodes` has no production key-creating writer -- the move to
//! `hashes[key][CONTEXT_NODE_FIELD]` took it out in June 2026 -- and every production read of it
//! is an `.or_else()` fallback after `hashes`. So it is a DECODE PATH for blocks written before
//! that move, and the question for consolidating it away is not "does anything write it" but
//! "can the data still ARRIVE in a shard this binary serves".
//!
//! It is on the wire (`#[serde(default)]`) and `ShardState` sets no `deny_unknown_fields`, so
//! deleting the field would make a stored `"context_nodes": {...}` decode into nothing, silently.
//! That is removable only if every door the data can come through refuses the artefacts that
//! carry it.
//!
//! # WHAT IS MEASURED HERE, AND WHY THREE ARMS AND NOT ONE
//!
//! The stamp (`index_format_version` against `SHARD_INDEX_FORMAT_VERSION`) is the obvious gate,
//! and `persistence::load_index_inner` does refuse a stale one BEFORE the index-log fold runs.
//! Checking only that loader answers the question for ONE door and reads as if it answered it for
//! all of them. `decode_index_bytes` has other callers.
//!
//!   * [`a_stale_stamped_base_index_does_not_deliver_a_context_node_entry`] -- the base-index
//!     door, stale stamp. REFUSED, and the entry does not arrive.
//!   * [`a_current_stamped_base_index_does_deliver_a_context_node_entry`] -- the same bytes at
//!     the CURRENT stamp. ACCEPTED, and the entry arrives. This is the positive control: without
//!     it the arm above passes just as well when the plant never worked, or when the reconcile
//!     drops the entry for a reason that has nothing to do with the stamp.
//!   * [`the_dump_manifest_door_takes_a_stale_stamped_index_image_with_no_refusal`] -- the dump
//!     manifest restore door. The index image it carries is handed to `decode_index_bytes`
//!     DIRECTLY, with no `index_format_version` comparison anywhere on that path, so a stale
//!     image installs and the entry arrives.
//!
//! The third arm is what decides the consolidation question, and it decides it the other way from
//! the stamp reasoning: the map is NOT removable on the ground that the stamp forecloses its
//! population, because one of the doors is not stamped at all.

#![allow(clippy::all)]
use super::*;

use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};

/// The key the planted entry is filed under. Deliberately NOT the key the string write below
/// uses: `apply_key_states` treats a key-state blob with no `context_nodes` field as a tombstone
/// for that key, so planting under the written key would be removed by the delta fold for a
/// reason that is not the stamp and would make both arms say the wrong thing.
const PLANTED: &str = "ctx:node:legacy";

/// The key the string write uses, so the base index has a live page whose entry can be cloned.
const WRITTEN: &str = "stamped";

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

/// Write one string on shard 7 and flush the base index, so there is a real index file with a
/// real live page in it.
fn write_and_flush(engine: &TemporalEngine) {
    engine.load_shard(7);
    let answer = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: WRITTEN.to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(answer.status.ok, "the setup write failed: {}", answer.status.message);
    engine.flush_shard_index(7);
}

/// Rewrite shard 7's base index so it carries a `context_nodes` entry, at `stamp`.
///
/// The entry is a CLONE of the live string entry rather than a hand-built one: `ElementEntry`
/// has no public constructor, and more to the point a planted address that no slab holds would be
/// dropped by the load for a reason that is not the stamp.
fn plant_in_base_index(dir: &std::path::Path, stamp: u32) {
    let path = dir.join("indexes").join("shard-7.index.json");
    let bytes = std::fs::read(&path).expect("a flushed base index exists");
    let mut restored = crate::engine::decode_index_bytes(&bytes).expect("the base index decodes");
    assert_eq!(
        restored.index_format_version,
        crate::engine::SHARD_INDEX_FORMAT_VERSION,
        "denominator: the index just written must carry the CURRENT stamp, or changing it below \
         changes nothing"
    );
    let live = restored
        .strings
        .get(WRITTEN)
        .expect("the written string is in the flushed index")
        .clone();
    restored.context_nodes.insert(PLANTED.into(), live);
    restored.index_format_version = stamp;
    std::fs::write(&path, crate::engine::encode_index_bytes(&restored)).expect("write");
}

/// Is `PLANTED` in shard 7's `context_nodes` map, as the engine now holds it?
fn planted_arrived(engine: &TemporalEngine) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&7)
        .map_or(false, |shard| shard.context_nodes.contains_key(PLANTED))
}

/// THE BASE-INDEX DOOR IS STAMPED, AND A STALE STAMP KEEPS THE ENTRY OUT.
#[test]
fn a_stale_stamped_base_index_does_not_deliver_a_context_node_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    write_and_flush(&engine);
    plant_in_base_index(
        dir.path(),
        crate::engine::SHARD_INDEX_FORMAT_VERSION - 1,
    );
    drop(engine);

    reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(7);
    // Bound by position against the accessor's own signature, not by the order that reads nicely:
    // all four are `u64` and swapping two compiles.
    let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
    assert!(
        stale_n > 0,
        "an index stamped {} against a current {} must be counted as a STALE-STAMP refusal; got \
         stale {stale_n} decode {decode_n} absent {absent_n} accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION - 1,
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    assert!(
        !planted_arrived(&reloaded),
        "a refused index must deliver NO part of itself, and this one delivered a \
         `context_nodes` entry"
    );
}

/// THE POSITIVE CONTROL. The same plant at the CURRENT stamp DOES arrive, which is what makes
/// the arm above a statement about the stamp rather than about the plant.
#[test]
fn a_current_stamped_base_index_does_deliver_a_context_node_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    write_and_flush(&engine);
    plant_in_base_index(dir.path(), crate::engine::SHARD_INDEX_FORMAT_VERSION);
    drop(engine);

    reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(7);
    let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
    assert!(
        accepted_n > 0,
        "an index at the current stamp must be ACCEPTED; got stale {stale_n} decode {decode_n} \
         absent {absent_n} accepted {accepted_n}"
    );
    assert!(
        planted_arrived(&reloaded),
        "an ACCEPTED index carrying a `context_nodes` entry must deliver it -- if it does not, \
         the refusal arm above is measuring the plant and not the stamp"
    );
}

/// THE DUMP-MANIFEST DOOR IS NOT STAMPED AT ALL, AND THAT IS WHAT DECIDES THE QUESTION.
///
/// `install_bucket_dump_manifest` hands `manifest.index_bytes` to `decode_index_bytes` and uses
/// what comes back. `decode_index_bytes` compares a version only on the binary container arm, and
/// a plain-JSON image -- which is what every store written with the container switched off
/// carries -- has no such comparison. Nothing on this path reads `index_format_version`.
///
/// So the stale image installs, and the `context_nodes` entry inside it becomes live state. The
/// manifest's own integrity checks are recomputed here rather than bypassed: this is a legitimate
/// manifest carrying a stale index image, which is exactly what a manifest written before the
/// stamp existed is.
#[test]
fn the_dump_manifest_door_takes_a_stale_stamped_index_image_with_no_refusal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    write_and_flush(&engine);

    // Put the entry into the live shard and flush, so the manifest's exported image carries it.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&7).expect("shard 7 is loaded");
        let live = shard
            .strings
            .get(WRITTEN)
            .expect("the written string is resident")
            .clone();
        shard.context_nodes.insert(PLANTED.into(), live);
    }
    engine.flush_shard_index(7);

    let mut manifest = engine
        .create_bucket_dump_manifest(7, Vec::new())
        .expect("a dump manifest is created for a loaded shard");

    // STALE THE IMAGE INSIDE THE MANIFEST, then make the manifest honest about it again.
    let mut image = crate::engine::decode_index_bytes(&manifest.index_bytes)
        .expect("the manifest index image decodes");
    assert!(
        image.context_nodes.contains_key(PLANTED),
        "denominator: the exported image must carry the planted entry, or the install below \
         proves nothing"
    );
    image.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION - 1;
    manifest.index_bytes = crate::engine::encode_index_bytes(&image);
    manifest.index_sha256 = super::super::sha256_hex_bytes(&manifest.index_bytes);
    // AND THE GENERATION ID, WHICH THE PRODUCER DERIVES FROM THE INDEX CHECKSUM, in the producer's
    // own order: id over the manifest fields including `index_sha256`, THEN the checksum over the
    // whole manifest. Skipping this is what `slot_dump_generation_mismatch` caught on the first
    // run of this test -- an integrity binding, not a format-stamp check, and a manifest written
    // before the stamp existed is self-consistent in exactly this way.
    manifest.dump_generation_id =
        super::super::bucket_dump_io::bucket_dump_generation_id(&manifest);
    manifest.checksum =
        super::super::bucket_dump_io::bucket_dump_manifest_checksum_in_place(&mut manifest)
            .expect("the manifest checksum recomputes");

    // Take the entry back out of the live shard, so what the install delivers is what the
    // manifest carried and not what was already there.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&7).expect("shard 7 is loaded");
        shard.context_nodes.remove(PLANTED);
    }
    assert!(
        !planted_arrived(&engine),
        "denominator: the live shard must NOT hold the entry before the install"
    );

    let installed = engine.install_bucket_dump_manifest(&manifest);
    assert!(
        installed.is_ok(),
        "the manifest carrying a stale-stamped index image was refused: {:?} -- if the refusal \
         names the format stamp, this door IS stamped and the finding this test records is wrong",
        installed.err()
    );
    assert!(
        planted_arrived(&engine),
        "the dump-manifest door delivered no `context_nodes` entry from an image that carries \
         one; if that is because the stamp was checked, this door is stamped after all"
    );
}
