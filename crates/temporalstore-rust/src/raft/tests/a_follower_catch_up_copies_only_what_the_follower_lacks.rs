// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A follower catch-up costs what the follower lacks, not what the leader holds.
//!
//! `catch_up` runs after every replicated write, once per follower, under the cluster write lock.
//! It copied the WHOLE leader log into the follower, so a write cost more the longer the log had
//! grown: over a local three-process cluster a one-cell write went from 26 ms at 60 keys to 37 ms
//! at 1,600, and writes that run one at a time under that lock could not go faster by adding
//! clients.
//!
//! The guard compares bytes allocated by one `catch_up` of an up-to-date follower at two log
//! lengths. It is a RELATIVE bound: the growth with the change on must be a small fraction of the
//! growth with the old whole-log copy switched back on in the same harness, so it needs no
//! absolute figure and cannot pass when the instrument sees nothing.

use super::*;

#[cfg(feature = "alloc-probe")]
const VALUE_BYTES: usize = 256;

#[cfg(feature = "alloc-probe")]
fn cluster_with_a_log(entries: usize) -> RaftCluster {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    for index in 0..entries {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; VALUE_BYTES],
            })
            .expect("propose must succeed");
    }
    cluster
}

/// Bytes allocated by one `catch_up(2)` after the follower already holds the log.
#[cfg(feature = "alloc-probe")]
fn catch_up_bytes(entries: usize, whole_log: bool) -> u64 {
    let cluster = cluster_with_a_log(entries);
    // One catch-up first, so the follower is level with the leader and what the next one costs
    // is the steady state, not the first fill.
    cluster.catch_up(2).expect("catch_up");
    crate::raft::cluster_membership::TEST_COPY_WHOLE_LOG
        .store(whole_log, std::sync::atomic::Ordering::Relaxed);
    let probe = crate::alloc_probe::Probe::start();
    cluster.catch_up(2).expect("catch_up");
    let bytes = probe.stop().alloc_bytes;
    crate::raft::cluster_membership::TEST_COPY_WHOLE_LOG
        .store(false, std::sync::atomic::Ordering::Relaxed);
    bytes
}

#[cfg(feature = "alloc-probe")]
#[test]
fn a_follower_catch_up_does_not_cost_more_as_the_log_grows() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(canary.stop().allocs > 0, "counting allocator not installed");
    drop(sink);

    let (short, long) = (100usize, 1_000usize);
    // Warm anything built once, then measure.
    let _ = catch_up_bytes(short, false);
    let (narrow_short, narrow_long) = (catch_up_bytes(short, false), catch_up_bytes(long, false));
    let (whole_short, whole_long) = (catch_up_bytes(short, true), catch_up_bytes(long, true));
    println!(
        "catch_up bytes: tail only {narrow_short} -> {narrow_long}; whole log {whole_short} -> {whole_long}"
    );
    // THE CONTROL: the whole-log copy must grow with the log through this same harness, or a
    // flat line proves nothing.
    let control_growth = whole_long.saturating_sub(whole_short);
    assert!(
        control_growth >= (long - short) as u64 * VALUE_BYTES as u64 / 2,
        "the whole-log control grew by {control_growth} bytes for {} more entries of {VALUE_BYTES} \
         bytes, so this instrument cannot see a log copy",
        long - short
    );
    let growth = narrow_long.saturating_sub(narrow_short);
    assert!(
        growth * 20 < control_growth,
        "a catch-up of an up-to-date follower allocated {narrow_short} bytes at {short} entries \
         and {narrow_long} at {long}; the whole-log copy grows by {control_growth}"
    );
}

/// What the follower holds after a catch-up is the log the whole-log copy leaves, whether it was
/// level or behind by a few entries.
#[test]
fn a_follower_catch_up_leaves_the_leader_log_in_the_follower() {
    for behind in [0usize, 1, 5] {
        for whole in [false, true] {
            let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
            for index in 0..20 {
                cluster
                    .propose(Command::StringSet {
                        key: format!("key-{index}"),
                        value: vec![b'x'; 32],
                    })
                    .expect("propose");
            }
            cluster.catch_up(2).expect("catch_up");
            for index in 0..behind {
                cluster
                    .propose(Command::StringSet {
                        key: format!("later-{index}"),
                        value: vec![b'y'; 32],
                    })
                    .expect("propose");
            }
            crate::raft::cluster_membership::TEST_COPY_WHOLE_LOG
                .store(whole, std::sync::atomic::Ordering::Relaxed);
            cluster.catch_up(2).expect("catch_up");
            crate::raft::cluster_membership::TEST_COPY_WHOLE_LOG
                .store(false, std::sync::atomic::Ordering::Relaxed);
            let leader = cluster.status().leader_id;
            assert_eq!(
                cluster.node_log_index_terms_for_test(2).unwrap(),
                cluster.node_log_index_terms_for_test(leader).unwrap(),
                "behind {behind}, whole-log copy {whole}"
            );
        }
    }
}
