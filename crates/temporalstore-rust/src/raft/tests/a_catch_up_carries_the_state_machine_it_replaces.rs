// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A catch-up replaces a node's log, so it must carry the state machine that log built.
//!
//! `install_meta_leader_snapshot_tail` is the metaserver's catch-up: it hands a node the leader's
//! log, commit point and `MetaState` in one step. A metaserver node carries TWO state machines,
//! though -- the `MetaState` summary AND `SingleNodeMeta`, the metadata store that actually answers
//! reads -- and the installer assigned the first while leaving the second untouched.
//!
//! Three of the four callers therefore left `node.meta` describing a log the node no longer has.
//! The fourth, the joint-consensus path, repairs it immediately afterwards with its own
//! `node.meta = leader_meta.clone()`, which is the shape every caller needs.
//!
//! That field is not a spare copy. `read_meta` and `with_readable_meta` pick the LOWEST-id node
//! whose commit point has reached the leader's and answer from ITS `node.meta`, so an unreconciled
//! node is not merely inconsistent -- it is the PREFERRED source for every metadata read. A node
//! added to a cluster whose leader has already snapshotted starts with an empty store, is marked
//! alive, and is handed the leader's commit point, so it passes that filter at once while holding
//! nothing: the entries it would have replayed from were compacted out of the log it was given.
//!
//! The apply loop cannot repair it either. `apply_meta_committed` is guarded by
//! `node.applied.insert(index)`, a set rather than a contiguous cursor, and the installer seeds
//! that set with `1..=leader_snapshot_index` without applying anything -- so every one of those
//! indexes is permanently marked done against an empty store.
//!
//! The same installer also ASSIGNED the commit point rather than raising it, so a node admitted for
//! being behind on its log while ahead on its commit point was pulled backwards.
//!
//! Each arm pins one clause: the metadata that must survive a node joining, the commit point that
//! must not fall, and -- as the inverse arm -- that a node genuinely behind is still caught up.

#![allow(clippy::all)]
use super::*;
use crate::meta::{MetaMutation, RegisterShardRequest};

const SHARD: ShardId = 11;

fn registration(addr: &str) -> RegisterShardRequest {
    RegisterShardRequest {
        shard_id: SHARD,
        server_addr: addr.to_string(),
        registered_at_ms: 0,
    }
}

fn meta_entry(index: u64, term: u64, addr: &str) -> MetaLogEntry {
    MetaLogEntry {
        term,
        index,
        command: MetaCommand::ApplyMutation(MetaMutation::RegisterShard(registration(addr))),
    }
}

/// What the metadata STORE answers -- the artefact a client reads, not the `MetaState` summary
/// beside it. The two are written by the same apply and only one was being carried.
fn addr_in_the_metadata_store(node: &MetaRaftNode) -> Option<String> {
    node.meta
        .get(SHARD)
        .location
        .map(|location| location.server_addr)
}

#[test]
fn a_node_added_after_a_snapshot_still_answers_committed_metadata() {
    // Node ids chosen so the node added LAST holds the lowest id: that is the one `read_meta`
    // prefers, which is what turns an unreconciled store into the answer every client gets.
    let cluster = MetaRaftCluster::new([5, 6]);
    assert!(
        cluster.register(registration("host-a:1")).status.ok,
        "fixture: the registration must commit"
    );

    // Vacuity floor: unless the cluster really answers this before the join, the arm below is
    // asserting nothing about the join.
    assert_eq!(
        cluster.get(SHARD).location.map(|l| l.server_addr),
        Some("host-a:1".to_string()),
        "fixture: committed metadata must be readable BEFORE a node joins"
    );

    // Snapshot and compact, so the log the joiner is handed no longer contains the registration.
    let snapshot = cluster.create_snapshot().expect("leader must be live");
    assert!(
        snapshot.last_included_index > 0,
        "fixture: the snapshot must cover the committed registration"
    );
    cluster
        .install_snapshot(5, snapshot.clone())
        .expect("the leader accepts its own snapshot");
    cluster
        .install_snapshot(6, snapshot)
        .expect("the follower accepts the snapshot");

    cluster.add_node(1).expect("a node may join a live cluster");

    assert_eq!(
        cluster.get(SHARD).location.map(|l| l.server_addr),
        Some("host-a:1".to_string()),
        "committed metadata stopped being readable once a node joined: the joiner was handed the \
         leader's commit point and a compacted log, so it answers reads from an empty store"
    );
    assert!(
        !cluster
            .list_shards(crate::meta::ListShardsRequest::default())
            .shards
            .is_empty(),
        "the shard listing went empty once a node joined"
    );
}

#[test]
fn a_meta_catch_up_does_not_pull_a_node_backwards() {
    // Ahead on the commit point, behind on the last log index -- the disjunction in
    // `catch_up_live_followers` admits exactly this node.
    let mut node = new_meta_node(2, RaftRole::Follower);
    for index in 1..=3 {
        node.log.push(meta_entry(index, 1, "host-own:1"));
    }
    node.commit_index = 3;
    apply_meta_committed(&mut node);
    assert_eq!(
        node.applied.iter().next_back().copied().unwrap_or_default(),
        3,
        "fixture: the node must really have applied through 3"
    );

    let leader_log: Vec<MetaLogEntry> = (1..=5)
        .map(|index| meta_entry(index, 2, "host-leader:1"))
        .collect();
    // The leader's own store, whatever it holds: this arm is about the commit point, and a
    // catch-up now carries the store alongside it.
    let leader_meta = SingleNodeMeta::default();
    install_meta_leader_snapshot_tail(
        &mut node,
        0,
        0,
        leader_log,
        1,
        MetaState::default(),
        leader_meta,
    );

    assert!(
        node.commit_index >= 3,
        "a catch-up must not lower the commit point: 3 -> {}",
        node.commit_index
    );
    assert!(
        node.applied.iter().next_back().copied().unwrap_or_default() <= node.commit_index,
        "the apply point must never exceed the commit point ({:?} > {})",
        node.applied.iter().next_back(),
        node.commit_index
    );
}

#[test]
fn a_node_genuinely_behind_is_still_caught_up() {
    // The inverse arm. A refusal that also refused the legitimate catch-up would satisfy both
    // arms above, so this one fails if the fix simply stopped installing.
    let cluster = MetaRaftCluster::new([1, 2]);
    assert!(cluster.register(registration("host-a:1")).status.ok);
    cluster.set_alive(2, true).expect("node 2 exists");

    let caught_up = cluster
        .catch_up_live_followers()
        .expect("the leader is live");
    assert!(
        caught_up.contains(&2),
        "a follower behind the leader must still be reported caught up, got {caught_up:?}"
    );
    cluster.catch_up(2).expect("an explicit catch-up still works");

    assert_eq!(
        cluster.commit_index(2).expect("node 2 exists"),
        cluster.commit_index(1).expect("node 1 exists"),
        "a caught-up follower must have reached the leader's commit point"
    );
    assert_eq!(
        cluster.get(SHARD).location.map(|l| l.server_addr),
        Some("host-a:1".to_string()),
        "the caught-up follower must be able to answer the committed registration"
    );
}
