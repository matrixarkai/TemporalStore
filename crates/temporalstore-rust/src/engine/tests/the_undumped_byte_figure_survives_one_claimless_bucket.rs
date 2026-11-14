// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE UNDUMPED-BYTES FIGURE MUST NOT FALL BACK TO THE DUMP-ZEROED COUNTER BECAUSE ONE BUCKET HAS
//! NO CLAIM.
//!
//! A dirty bucket whose first-dirty WAL sequence is 0 cannot be placed in the log. The figure used
//! to give up on the whole shard for it and read the counter that any dump manifest zeroes, which is
//! the re-arm the claim-relative figure exists to avoid. The buckets that CAN be placed still bound
//! the answer from below.
#![allow(clippy::all)]
use super::*;

#[test]
fn one_bucket_without_a_claim_does_not_put_the_figure_back_on_the_zeroed_counter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let write = |from: usize, to: usize| {
        for index in from..to {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("claimless-{index:05}"),
                    value: vec![b'v'; 96],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    };
    write(0, 300);
    // A dump zeroes the counter without moving the oldest undumped write of a bucket it left out.
    engine.wal_store.mark_dumped(1);
    write(300, 600);

    let counter = engine.wal_store.undumped_len_since_dump(1);
    // NON-VACUITY: the counter holds only the second half, and the claim's figure holds both.
    assert!(counter > 0);
    let mut claims = std::collections::BTreeMap::new();
    claims.insert(7u32, (1u64, 1u64));
    claims.insert(9u32, (0u64, 0u64));
    let figure = engine.undumped_wal_bytes_from_claims(1, &claims);
    assert!(
        figure >= counter + counter / 2,
        "with one claimless bucket the figure is {figure}, no better than the zeroed counter          {counter}, so the buckets that could be placed were ignored"
    );
    // And with every bucket placed the answer is the claim's, not the counter's.
    claims.remove(&9u32);
    assert!(engine.undumped_wal_bytes_from_claims(1, &claims) >= counter + counter / 2);
}
