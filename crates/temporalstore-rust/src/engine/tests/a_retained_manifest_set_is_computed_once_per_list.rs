// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH DUMP MANIFESTS THE SHARD KEEPS IS WORKED OUT ONCE, NOT ONCE PER ROUND.
//!
//! The manifest prune plan names the manifests to keep by walking them newest first and building
//! the set of every bucket they cover. A drained shard's maintenance round asks it every time, and
//! nothing between two rounds has changed the manifests, so the answer repeats: measured on a
//! thousand-key shard, that one call was about half of a quiet round's instructions.
//!
//! The answer depends only on the manifests' content, a manifest is never rewritten, and the listing
//! hands out the same allocation while the file is unchanged, so the list of allocations is the
//! key (`retained_bucket_dump_manifest_ids_memoised`).
//!
//! ONE TEST, THREE ARMS, because each fails on a different mutation:
//!   * a second and third plan over the same manifests compute nothing (a memo that never hit fails);
//!   * the same plans with the memo off compute every time and say the same thing (a fixture that
//!     could not reach the computation fails, and a memo that changed the answer fails);
//!   * after a new dump the plan computes again and its answer equals the memo-off answer (a memo
//!     that went stale fails).
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn dump(engine: &TemporalEngine, which: &str) {
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new());
    assert!(manifest.is_ok(), "{which}: {:?}", manifest.err());
}

#[test]
fn the_retained_manifest_set_is_computed_once_across_rounds_and_again_after_a_new_dump() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    for index in 0..48 {
        set(&engine, &format!("r-{index:03}"), &[b'a'; 40]);
    }
    dump(&engine, "first dump");
    for index in 48..64 {
        set(&engine, &format!("r-{index:03}"), &[b'b'; 40]);
    }
    dump(&engine, "second dump");
    let listed = engine.list_bucket_dump_manifests(1).len();
    assert!(listed >= 2, "need several manifests for the walk to have something to keep, got {listed}");

    // ARM 1: WITH THE MEMO. The first plan may compute; the next two must not.
    let _warm = engine.bucket_dump_manifest_prune_plan(1);
    crate::engine::reset_retained_ids_memo_counts();
    let first = engine.bucket_dump_manifest_prune_plan(1);
    let second = engine.bucket_dump_manifest_prune_plan(1);
    let (hits, computed) = crate::engine::retained_ids_memo_counts();
    assert_eq!(computed, 0, "two plans over unchanged manifests computed the kept set {computed} times");
    assert_eq!(hits, 2, "the two plans did not both read the kept set from memory ({hits} hits)");
    assert_eq!(first, second, "a plan that read the kept set from memory said something else");

    // ARM 2: THE CONTROL. Memo off: every plan computes, and the answer must not move.
    let off = crate::engine::bucket_dump_io::ManifestParseMemoOff::new();
    crate::engine::reset_retained_ids_memo_counts();
    let third = engine.bucket_dump_manifest_prune_plan(1);
    let fourth = engine.bucket_dump_manifest_prune_plan(1);
    let (hits_off, computed_off) = crate::engine::retained_ids_memo_counts();
    drop(off);
    assert_eq!(hits_off, 0, "with the memo off nothing may be served from it");
    assert_eq!(computed_off, 2, "with the memo off each plan computes the kept set");
    assert_eq!(first, third, "the memo changed what the plan says");
    assert_eq!(first, fourth, "the memo changed what the plan says");

    // ARM 3: A NEW MANIFEST IS A DIFFERENT LIST. The plan computes again and agrees with the
    // memo-off answer for the same manifests.
    for index in 0..8 {
        set(&engine, &format!("r-{index:03}"), &[b'c'; 52]);
    }
    dump(&engine, "third dump");
    crate::engine::reset_retained_ids_memo_counts();
    let after = engine.bucket_dump_manifest_prune_plan(1);
    let (_, computed_after) = crate::engine::retained_ids_memo_counts();
    assert_eq!(computed_after, 1, "a new manifest must make the plan compute the kept set again");
    let off = crate::engine::bucket_dump_io::ManifestParseMemoOff::new();
    let after_off = engine.bucket_dump_manifest_prune_plan(1);
    drop(off);
    assert_eq!(after, after_off, "after a new dump the memoised plan disagrees with a fresh one");
}