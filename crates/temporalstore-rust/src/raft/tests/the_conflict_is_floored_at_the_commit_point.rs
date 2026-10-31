// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The conflict truncation, floored at the commit point.
//!
//! Raft section 5.3 lets an AppendEntries replace a log suffix where the terms disagree at an
//! index. What keeps that safe is a floor at the commit point: a conflict search that stops at
//! `commit_index`, and reports `commit_index` itself when nothing above it differs, can never name
//! an index at or below the commit point -- so the truncation that follows cannot cut a committed
//! entry, and `applied_index <= commit_index` holds by construction rather than by hope.
//!
//! Neither path here had that floor, and the consequence is not a lost truncation but a SILENT
//! DISAGREEMENT between the log and the state machine:
//!
//! * The data path keeps a monotonic exactly-once floor `max_applied_index` that a truncation
//!   deliberately does NOT lower. Both apply loops skip any entry at or below it, advancing the
//!   apply cursor over the entry WITHOUT running it. So a conflicting append at or below the
//!   commit point replaced a committed entry in the log while the state machine kept the command
//!   it superseded -- and `applied_index` reported that index as applied either way.
//! * The meta path truncated on a bare index collision without comparing terms at all, deleting
//!   committed suffix entries for a mere duplicate or reordered append, and never rewound
//!   `applied` -- so `applied.insert` returned false for the replacement and the apply skipped it.
//!
//! With the floor the harmful case is unreachable rather than papered over: an entry is only ever
//! applied at or below `commit_index`, so `max_applied_index <= commit_index` always holds, and a
//! truncation confined to indexes ABOVE the commit point can never reach the floor. Each arm
//! below pins one clause: the refusal, what it reports, the surviving agreement between log and
//! state machine, and -- as the inverse arm -- that a conflict above the commit point still
//! truncates and still runs.

#![allow(clippy::all)]
use super::*;
use crate::meta::{MetaMutation, RegisterShardRequest};

const SHARD: ShardId = 11;
const KEY: &str = "k";

fn set_entry(index: u64, term: u64, value: &str) -> RaftLogEntry {
    RaftLogEntry {
        leader_time_ms: 0,
        term,
        index,
        shard_id: SHARD,
        command: Command::StringSet {
            key: KEY.to_string(),
            value: value.as_bytes().to_vec(),
        },
    }
}

/// What the STATE MACHINE holds, read out of the engine rather than inferred from a cursor.
/// `applied_index` is the number asserted about the apply; this is the artefact itself.
fn value_in_the_state_machine(node: &RaftNode) -> Option<Vec<u8>> {
    let response = node.engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: KEY.to_string(),
        },
    });
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("expected a byte value back from the engine, got {other:?}"),
    }
}

fn meta_entry(index: u64, term: u64, addr: &str) -> MetaLogEntry {
    MetaLogEntry {
        term,
        index,
        command: MetaCommand::ApplyMutation(MetaMutation::RegisterShard(RegisterShardRequest {
            shard_id: SHARD,
            server_addr: addr.to_string(),
            registered_at_ms: 0,
        })),
    }
}

fn addr_in_the_meta_state(node: &MetaRaftNode) -> Option<String> {
    node.state
        .shards
        .get(&SHARD)
        .map(|location| location.server_addr.clone())
}

#[test]
fn a_conflicting_entry_at_the_commit_point_is_refused_rather_than_absorbed() {
    let mut node = new_node(1, RaftRole::Follower, SHARD);
    for index in 1..=3 {
        append_entry(&mut node, set_entry(index, 1, &format!("v{index}")));
    }
    node.commit_index = 3;
    apply_committed(&mut node);

    // Vacuity floor: unless the fixture really applied all three, nothing below tests the floor.
    assert_eq!(
        node.applied_index, 3,
        "fixture must have applied all three committed entries before the conflict arrives"
    );
    assert_eq!(
        node.max_applied_index, 3,
        "fixture must have raised the exactly-once floor to 3, or the skip branch under test \
         cannot be reached by anything"
    );
    assert_eq!(
        value_in_the_state_machine(&node),
        Some(b"v3".to_vec()),
        "fixture must have RUN the committed commands, not merely moved a cursor"
    );

    // The conflict: a DIFFERENT term carrying a DIFFERENT command, at an index this group has
    // already committed. Raft forbids overwriting a committed entry, so the sender is wrong.
    append_entry(&mut node, set_entry(3, 2, "replacement"));

    assert_eq!(
        node.pipeline_state.committed_overwrite_rejections, 1,
        "the refusal must be REPORTED, not merely performed: a silent refusal is \
         indistinguishable from a truncation that happened to leave the log alone"
    );
    assert_eq!(
        node.log.len(),
        3,
        "nothing may be truncated at or below the commit point; log is {:?}",
        node.log
            .iter()
            .map(|entry| (entry.index, entry.term))
            .collect::<Vec<_>>()
    );
    let at_the_commit_point = node
        .log
        .iter()
        .find(|entry| entry.index == 3)
        .expect("index 3 must still be present");
    assert_eq!(
        at_the_commit_point.term, 1,
        "the committed entry must still be the one that was committed, not the replacement"
    );
    // The decisive assertion, and the one the defect defeated: the log and the state machine must
    // say the SAME thing about index 3.
    assert_eq!(
        value_in_the_state_machine(&node),
        Some(b"v3".to_vec()),
        "state machine must still hold the committed command"
    );
    assert!(
        node.max_applied_index <= node.commit_index,
        "the exactly-once floor must never exceed the commit point ({} > {}), or an unapplied \
         entry can be skipped as though it had run",
        node.max_applied_index,
        node.commit_index
    );
}

#[test]
fn the_commit_floor_still_lets_a_conflict_above_it_truncate_and_run() {
    let mut node = new_node(1, RaftRole::Follower, SHARD);
    for index in 1..=3 {
        append_entry(&mut node, set_entry(index, 1, &format!("v{index}")));
    }
    // Commit BELOW the conflicting index: entry 3 is uncommitted, so replacing it is exactly what
    // Raft section 5.3 is for and the floor must not interfere.
    node.commit_index = 2;
    apply_committed(&mut node);
    assert_eq!(
        node.applied_index, 2,
        "fixture must apply only up to the commit point"
    );

    append_entry(&mut node, set_entry(3, 2, "replacement"));

    assert_eq!(
        node.pipeline_state.committed_overwrite_rejections, 0,
        "the refusal must NOT fire above the commit point, or the floor has disabled the \
         truncation Raft requires"
    );
    let at_three = node
        .log
        .iter()
        .find(|entry| entry.index == 3)
        .expect("index 3 must be present");
    assert_eq!(
        at_three.term, 2,
        "an uncommitted conflicting entry must replace the one it conflicts with"
    );

    node.commit_index = 3;
    apply_committed(&mut node);
    assert_eq!(
        value_in_the_state_machine(&node),
        Some(b"replacement".to_vec()),
        "the replacement must actually RUN once committed -- skipping it is the defect this \
         floor exists to make unreachable"
    );
}

#[test]
fn a_same_term_duplicate_meta_append_no_longer_deletes_the_suffix() {
    let mut node = new_meta_node(1, RaftRole::Follower);
    for index in 1..=3 {
        append_meta_entry(&mut node, meta_entry(index, 1, &format!("addr{index}")));
    }
    node.commit_index = 3;
    apply_meta_committed(&mut node);
    assert_eq!(
        node.log.len(),
        3,
        "fixture must hold three entries before the duplicate arrives"
    );

    // A duplicate or reordered append of an entry already present, at the SAME term: it is the
    // same entry, so it must be a no-op. Truncating on the bare index collision deleted index 3.
    append_meta_entry(&mut node, meta_entry(2, 1, "addr2"));

    assert_eq!(
        node.log.len(),
        3,
        "a same-term duplicate must not truncate: log is {:?}",
        node.log
            .iter()
            .map(|entry| (entry.index, entry.term))
            .collect::<Vec<_>>()
    );
    assert!(
        node.log.iter().any(|entry| entry.index == 3),
        "the committed suffix entry at index 3 must survive a duplicate append at index 2"
    );
    assert_eq!(
        addr_in_the_meta_state(&node),
        Some("addr3".to_string()),
        "the state machine must still hold what the surviving suffix applied"
    );
}

#[test]
fn a_conflicting_meta_entry_at_the_commit_point_is_refused() {
    let mut node = new_meta_node(1, RaftRole::Follower);
    for index in 1..=3 {
        append_meta_entry(&mut node, meta_entry(index, 1, &format!("addr{index}")));
    }
    node.commit_index = 3;
    apply_meta_committed(&mut node);
    assert_eq!(
        addr_in_the_meta_state(&node),
        Some("addr3".to_string()),
        "fixture must have applied the committed entries"
    );

    append_meta_entry(&mut node, meta_entry(3, 2, "replacement"));

    assert_eq!(
        node.log.len(),
        3,
        "nothing may be truncated at or below the commit point"
    );
    let at_the_commit_point = node
        .log
        .iter()
        .find(|entry| entry.index == 3)
        .expect("index 3 must still be present");
    assert_eq!(
        at_the_commit_point.term, 1,
        "the committed meta entry must survive a conflicting append at its own index"
    );
    apply_meta_committed(&mut node);
    assert_eq!(
        addr_in_the_meta_state(&node),
        Some("addr3".to_string()),
        "log and state machine must agree: before the floor the log held the replacement while \
         the state machine kept what it superseded, because `applied` still contained index 3 \
         and the apply skipped it"
    );
}

#[test]
fn a_conflicting_meta_entry_above_the_commit_point_runs_the_replacement() {
    let mut node = new_meta_node(1, RaftRole::Follower);
    for index in 1..=3 {
        append_meta_entry(&mut node, meta_entry(index, 1, &format!("addr{index}")));
    }
    node.commit_index = 2;
    apply_meta_committed(&mut node);
    assert_eq!(
        addr_in_the_meta_state(&node),
        Some("addr2".to_string()),
        "fixture must apply only up to the commit point"
    );

    append_meta_entry(&mut node, meta_entry(3, 2, "replacement"));
    let at_three = node
        .log
        .iter()
        .find(|entry| entry.index == 3)
        .expect("index 3 must be present");
    assert_eq!(
        at_three.term, 2,
        "an uncommitted conflicting meta entry must be replaced"
    );

    node.commit_index = 3;
    apply_meta_committed(&mut node);
    assert_eq!(
        addr_in_the_meta_state(&node),
        Some("replacement".to_string()),
        "the replacement must RUN -- the rewind of `applied` is what lets it"
    );
}

#[test]
fn a_same_term_duplicate_above_the_commit_point_does_not_delete_the_suffix() {
    // The arm that makes the term compare LOAD-BEARING. In the committed case the commit floor
    // refuses a duplicate anyway, so removing the term compare still leaves the suffix intact --
    // the two guards are redundant maintainers there and the test would pass for the wrong
    // reason. Above the commit point the floor does not apply, so the term compare is the only
    // thing standing between a duplicate append and a deleted suffix.
    let mut node = new_meta_node(1, RaftRole::Follower);
    for index in 1..=3 {
        append_meta_entry(&mut node, meta_entry(index, 1, &format!("addr{index}")));
    }
    node.commit_index = 1;
    apply_meta_committed(&mut node);
    assert_eq!(
        node.log.len(),
        3,
        "fixture must hold three entries, only the first of them committed"
    );

    // Index 2 is ABOVE the commit point, so the floor cannot save the suffix here.
    append_meta_entry(&mut node, meta_entry(2, 1, "addr2"));

    assert_eq!(
        node.log.len(),
        3,
        "a same-term duplicate above the commit point must be a no-op; log is {:?}",
        node.log
            .iter()
            .map(|entry| (entry.index, entry.term))
            .collect::<Vec<_>>()
    );
    assert!(
        node.log.iter().any(|entry| entry.index == 3),
        "the uncommitted suffix entry at index 3 must survive a duplicate append at index 2 -- \
         without the term compare the bare index collision truncates it away"
    );
}
