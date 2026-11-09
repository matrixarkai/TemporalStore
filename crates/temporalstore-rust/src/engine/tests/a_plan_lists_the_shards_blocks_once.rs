// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WAL RECLAIM PLAN LISTS THE SHARD'S LIVE BLOCKS ONCE, NOT ONCE PER THING IT DERIVES FROM THEM.
//!
//! The plan needs the per-bucket summaries and the live shard's per-bucket fingerprint digests. Both
//! are folds over the same list of live blocks, and each used to list every block of the shard for
//! itself, in two lock acquisitions. They are now one listing under one read of the shard.
//!
//! THREE CLAIMS, EACH WITH A CONTROL.
//!
//!   1. THE ANSWERS ARE THE SEPARATE CALLS' ANSWERS, bucket for bucket, for both folds.
//!   2. IT IS ONE LISTING: the entries visited equal the live block count, where the two separate
//!      calls (the control) visit twice that.
//!   3. NOTHING IS REMEMBERED BETWEEN USES. A write planted between two calls moves the answer, and
//!      the second call still equals the separate calls taken after the write. A plan that kept the
//!      first listing would pass claims 1 and 2 and fail this one.
//!
//! The counters are process-wide, like those the neighbouring scale tests read, so this file is run
//! single-threaded as they are.
#![allow(clippy::all)]
use super::*;
use crate::engine::storage_reporting::bucket_generation_digests_by_bucket;

fn engine_with_keys(dir: &std::path::Path, count: usize) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    let mut index = 0usize;
    while index < count {
        let end = (index + 500).min(count);
        let commands = (index..end)
            .map(|cursor| Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; 128],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
    engine
}

fn separate_digests(engine: &TemporalEngine) -> std::collections::BTreeMap<u32, String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    bucket_generation_digests_by_bucket(1, shard)
        .into_iter()
        .map(|(bucket, digest)| (bucket, format!("{digest:?}")))
        .collect()
}

fn merged_digests(
    digests: std::collections::BTreeMap<u32, crate::engine::storage_reporting::BucketFingerprintDigest>,
) -> std::collections::BTreeMap<u32, String> {
    digests
        .into_iter()
        .map(|(bucket, digest)| (bucket, format!("{digest:?}")))
        .collect()
}

#[test]
fn the_merged_listing_answers_what_the_two_separate_calls_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_keys(dir.path(), 900);

    let (summaries, digests) = engine.bucket_storage_summaries_with_digests(1, true);
    assert_eq!(summaries, engine.bucket_storage_summaries(1));
    assert_eq!(merged_digests(digests), separate_digests(&engine));

    // CONTROL: two empty answers would agree.
    assert!(summaries.len() >= 100, "only {} summaries", summaries.len());
    assert!(summaries.iter().any(|summary| summary.block_ref_count > 0));
    // `with_digests = false` is the summaries call and reads no digest.
    let (plain, none) = engine.bucket_storage_summaries_with_digests(1, false);
    assert_eq!(plain, summaries);
    assert!(none.is_empty());
}

#[test]
fn the_merged_listing_visits_each_live_block_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_keys(dir.path(), 900);

    crate::engine::reset_live_block_scan_entries();
    let _ = engine.bucket_storage_summaries(1);
    let _ = separate_digests(&engine);
    let separate = crate::engine::live_block_scan_entries();
    assert!(separate >= 1_800, "the separate calls visited only {separate} entries");

    crate::engine::reset_live_block_scan_entries();
    let _ = engine.bucket_storage_summaries_with_digests(1, true);
    let merged = crate::engine::live_block_scan_entries();
    assert_eq!(
        merged * 2,
        separate,
        "merged visited {merged} entries, the two separate calls {separate}"
    );
}

#[test]
fn a_write_between_two_uses_moves_the_second_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_keys(dir.path(), 900);

    let (summaries_before, digests_before) = engine.bucket_storage_summaries_with_digests(1, true);
    let digests_before = merged_digests(digests_before);

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k-00000007".to_string(),
            value: vec![b'w'; 300],
        },
    });
    assert!(response.status.ok, "planted write failed: {:?}", response.status);

    let (summaries_after, digests_after) = engine.bucket_storage_summaries_with_digests(1, true);
    let digests_after = merged_digests(digests_after);
    assert_ne!(
        digests_before, digests_after,
        "the planted write left every bucket's digest unchanged: the second call reused the first listing"
    );
    assert_ne!(summaries_before, summaries_after);
    // And the second call is the separate calls' answer for the shard as it now is.
    assert_eq!(summaries_after, engine.bucket_storage_summaries(1));
    assert_eq!(digests_after, separate_digests(&engine));
}
