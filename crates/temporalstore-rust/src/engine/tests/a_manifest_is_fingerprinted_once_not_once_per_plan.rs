// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A RETAINED DUMP MANIFEST IS FINGERPRINTED ONCE, NOT ONCE PER PLAN.
//!
//! The WAL reclaim plan asks, for every bucket, whether some retained dump manifest still describes
//! the bucket as it is now. Answering means decoding the manifest's whole index image and walking
//! every live block in it, and the plan did that for every manifest it consulted on every call. A
//! drained shard consults all of them: its buckets match no manifest, so the search runs to the
//! oldest. Measured on a 100-key shard with 47 retained manifests, 36.7% of one idle round's
//! instructions were `decode_index_bytes`.
//!
//! The plan compares a bucket's fingerprints and nothing else, and a manifest is never rewritten,
//! so what is kept per manifest is a 16-byte digest per bucket, looked up by the manifest's own
//! allocation (`manifest_digests_memoised`).
//!
//! TWO GUARDS, BECAUSE THEY FAIL ON DISJOINT MUTATIONS.
//!   * `a_digest_equals_exactly_when_the_lines_were_equal` holds the digest to the answer the
//!     strings gave, bucket by bucket, for every (manifest, live shard) pair across a history of
//!     writes, overwrites, deletes and dumps. A digest that dropped a term of the line passes the
//!     other guard and fails this one.
//!   * `a_retained_manifest_is_decoded_once_across_plans` is the speed-up: the second plan over the
//!     same manifests computes nothing. A memo that never hit passes the first guard and fails this
//!     one. It carries its own control (the same two plans with the memo off, which must compute
//!     every time) and the answer must not move.
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

fn run(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    run(
        engine,
        Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    );
}

fn dump(engine: &TemporalEngine, which: &str) {
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new());
    assert!(manifest.is_ok(), "{which}: {:?}", manifest.err());
}

/// Three manifests taken at different points of a history, then a live shard that has moved on
/// from all of them: some buckets unchanged since the newest dump, some rewritten after it, some
/// emptied, some new. Every kind of (manifest, bucket) pair the plan can meet.
fn a_history_with_three_manifests(engine: &TemporalEngine) {
    for index in 0..48 {
        set(engine, &format!("d-{index:03}"), &[b'a'; 40]);
    }
    dump(engine, "first dump");
    for index in 0..16 {
        set(engine, &format!("d-{index:03}"), &[b'b'; 52]);
    }
    for index in 48..64 {
        set(engine, &format!("d-{index:03}"), &[b'c'; 40]);
    }
    dump(engine, "second dump");
    for index in 0..8 {
        run(
            engine,
            Command::StringDelete {
                key: format!("d-{:03}", 16 + index),
            },
        );
    }
    set(engine, "d-000", &[b'd'; 70]);
    dump(engine, "third dump");
    // And the shard moves on past the newest manifest.
    for index in 0..12 {
        set(engine, &format!("d-{:03}", 30 + index), &[b'e'; 33]);
    }
    for index in 64..72 {
        set(engine, &format!("d-{index:03}"), &[b'f'; 40]);
    }
}

#[test]
fn a_digest_equals_exactly_when_the_lines_were_equal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    a_history_with_three_manifests(&engine);

    let manifests = engine.list_bucket_dump_manifests(1);
    assert!(
        manifests.len() >= 3,
        "the history must retain at least three manifests, got {}",
        manifests.len()
    );

    let shards = engine.shards.read().unwrap();
    let live = shards.get(&1).expect("shard 1");
    let live_lines = crate::engine::storage_reporting::bucket_generation_fingerprints_by_bucket(1, live);
    let live_digests = crate::engine::storage_reporting::bucket_generation_digests_by_bucket(1, live);
    assert_eq!(
        live_lines.keys().collect::<Vec<_>>(),
        live_digests.keys().collect::<Vec<_>>(),
        "the digest names exactly the buckets the lines named"
    );

    let (mut equal_pairs, mut unequal_pairs, mut absent_pairs) = (0usize, 0usize, 0usize);
    for manifest in &manifests {
        let state = crate::engine::decode_index_bytes(&manifest.index_bytes).expect("decodes");
        let lines = crate::engine::storage_reporting::bucket_generation_fingerprints_by_bucket(1, &state);
        let digests = crate::engine::storage_reporting::bucket_generation_digests_by_bucket(1, &state);
        assert_eq!(lines.keys().collect::<Vec<_>>(), digests.keys().collect::<Vec<_>>());
        let buckets = lines
            .keys()
            .chain(live_lines.keys())
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        for bucket in buckets {
            let by_lines = lines.get(&bucket) == live_lines.get(&bucket);
            let by_digest = digests.get(&bucket) == live_digests.get(&bucket);
            assert_eq!(
                by_lines, by_digest,
                "bucket {bucket} of manifest {}: the lines said equal={by_lines}, the digest said equal={by_digest}",
                manifest.manifest_id
            );
            match (lines.get(&bucket), live_lines.get(&bucket)) {
                (Some(_), Some(_)) if by_lines => equal_pairs += 1,
                (Some(_), Some(_)) => unequal_pairs += 1,
                _ => absent_pairs += 1,
            }
        }
    }
    // NON-VACUITY. A history that produced only equal pairs, or only unequal ones, would let a
    // digest that always agrees (or never does) satisfy the loop above.
    assert!(equal_pairs > 0, "no bucket kept its lines across a dump, so nothing was held equal");
    assert!(unequal_pairs > 0, "no bucket changed after a dump, so nothing was held unequal");
    assert!(absent_pairs > 0, "no bucket was present on one side only");
}

#[test]
fn a_retained_manifest_is_decoded_once_across_plans() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    a_history_with_three_manifests(&engine);
    let manifests = engine.list_bucket_dump_manifests(1).len();
    assert!(manifests >= 3, "need several retained manifests, got {manifests}");

    // WITH THE MEMO.
    crate::engine::reset_manifest_digest_memo_counts();
    let first = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let (hits_1, computed_1) = crate::engine::manifest_digest_memo_counts();
    let second = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let (hits_2, computed_2) = crate::engine::manifest_digest_memo_counts();
    // DENOMINATOR: the first plan did compute something, or the claim below is about nothing.
    assert!(
        computed_1 >= 2,
        "the first plan fingerprinted {computed_1} manifests; the fixture does not reach them"
    );
    assert_eq!(
        computed_2, computed_1,
        "the second plan computed {} more fingerprints for manifests it had already seen",
        computed_2 - computed_1
    );
    assert!(
        hits_2 > hits_1,
        "the second plan did not read a single fingerprint from memory ({hits_1} -> {hits_2})"
    );
    assert_eq!(first, second, "a plan that read its fingerprints from memory said something else");

    // THE CONTROL: the same two plans with the memo off must compute every time, and say the same.
    let off = crate::engine::bucket_dump_io::ManifestParseMemoOff::new();
    crate::engine::reset_manifest_digest_memo_counts();
    let third = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let (_, computed_3) = crate::engine::manifest_digest_memo_counts();
    let fourth = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let (_, computed_4) = crate::engine::manifest_digest_memo_counts();
    drop(off);
    assert_eq!(computed_3, computed_1, "with no memo the first plan computes what it computed with one");
    assert_eq!(computed_4, 2 * computed_3, "with no memo the second plan computes again");
    assert_eq!(first, third, "the memo changed what the plan says");
    assert_eq!(first, fourth, "the memo changed what the plan says");
}
