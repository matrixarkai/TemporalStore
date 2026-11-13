// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A failed expiry sweep is counted, and a successful one is not.
//!
//! The runtime consumes a sweep error and moves to the next shard, so a sweep that failed on every
//! tick looked exactly like a shard with nothing due: `expiry_sweeps` counts rounds, whatever each
//! shard returned. The failing sweep here names a shard this engine never loaded, which the sweep
//! refuses with `shard_not_loaded`; the successful sweep beside it runs against a loaded shard that
//! holds a key whose TTL has passed, so it is a sweep that did real work and must add nothing.
#![allow(clippy::all)]
use super::*;

use crate::engine::reports::ShardExpirySweepRequest;

#[test]
fn an_expiry_sweep_that_fails_is_counted_and_one_that_succeeds_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let written = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetEx {
            key: "expiry-failure-key".to_string(),
            value: vec![b'v'; 16],
            ttl_ms: 1,
        },
    });
    assert!(written.status.ok, "fixture write: {:?}", written.status);
    assert_eq!(engine.expiry_sweep_failures(), 0, "nothing has failed yet");

    for attempt in 1..=2u64 {
        let refused = engine.sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id: 99,
            load_cold_buckets: true,
            ..ShardExpirySweepRequest::default()
        });
        assert!(refused.is_err(), "a sweep of a shard that is not loaded succeeded");
        assert_eq!(
            engine.expiry_sweep_failures(),
            attempt,
            "failed sweep {attempt} was not counted"
        );
    }

    // THE CONTROL: a sweep that runs, against a key that is due, and removes it. A control that
    // swept nothing would not distinguish "counts only failures" from "counts nothing".
    std::thread::sleep(std::time::Duration::from_millis(20));
    let swept = engine
        .sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id: 1,
            load_cold_buckets: true,
            ..ShardExpirySweepRequest::default()
        })
        .expect("the control sweep of a loaded shard must succeed");
    assert_eq!(
        swept.expired_records_removed, 1,
        "the control sweep did not remove the due key, so it proves nothing about a sweep that works"
    );
    assert_eq!(
        engine.expiry_sweep_failures(),
        2,
        "a sweep that succeeded was counted as a failure"
    );
}
