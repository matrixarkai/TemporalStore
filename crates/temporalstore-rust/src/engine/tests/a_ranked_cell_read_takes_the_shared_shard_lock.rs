// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A ranked cell read is a READ of the shard: it is answered while other readers hold the shard
//! table, and it does not stop the writers while it decodes samples.
//!
//! It used to take the shard table's WRITE lock like a write, so every ranked read on a shard
//! queued behind every other and read throughput did not rise with threads. Point and range reads
//! already took the shared guard, copied the addresses they needed out under it and read the blocks
//! after it dropped; the ranked read now does the same.
//!
//! The guard holds one reader on the shard table and asks, from another thread, for a ranked read.
//! A reader that needs the write lock cannot be answered while it is held. THE CONTROL is a command
//! that does take the write lock, which must NOT be answered in the same state: without it a read
//! answered quickly proves nothing about the lock.

#![allow(clippy::all)]
use super::*;
use super::the_window_reduce_bytes_are_counted_per_operation::{open, rank_spec, seed_samples, stored_key};
use crate::types::{WindowReduceRankSpec, WindowReduceScore, WindowReduceWindow};
use std::sync::{mpsc, Arc};
use std::time::Duration;

fn rank_command(key: &str, spec: WindowReduceRankSpec) -> Command {
    Command::WindowReduceRank { key: key.to_string(), spec }
}

/// Runs `command` on another thread; `Some` if it was answered within `wait`.
fn answered_within(
    engine: &Arc<TemporalEngine>,
    command: Command,
    wait: Duration,
) -> (Option<ExecuteResponse>, std::thread::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel();
    let engine = Arc::clone(engine);
    let handle = std::thread::spawn(move || {
        let response = engine.execute(ExecuteRequest { shard_id: 1, command });
        let _ = sender.send(response);
    });
    (receiver.recv_timeout(wait).ok(), handle)
}

#[test]
fn a_ranked_cell_read_is_answered_while_another_reader_holds_the_shard_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(dir.path(), None));
    let key = stored_key(1);
    seed_samples(&engine, &key, 8, 16);

    let guard = engine.shards.read().expect("engine lock poisoned");

    let (answer, handle) = answered_within(&engine, rank_command(&key, rank_spec()), Duration::from_secs(5));
    let answer = answer.expect("a ranked read was not answered while a reader held the shard table");
    assert!(answer.status.ok, "{:?}", answer.status);
    match &answer.response {
        CommandResponse::WindowReduceRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "refused: {}", ranked.refused);
            assert!(!ranked.rows.is_empty(), "the read ranked nothing, so it proves nothing");
        }
        other => panic!("not a ranking: {other:?}"),
    }
    handle.join().unwrap();

    // THE CONTROL: a write needs the exclusive lock, so the same wait must expire.
    let write = Command::FeatureAppend {
        key: key.clone(),
        points: vec![FeaturePoint { timestamp_ms: 9_000, value: vec![0] }],
    };
    let (blocked, write_handle) = answered_within(&engine, write, Duration::from_millis(500));
    assert!(
        blocked.is_none(),
        "a write was answered while a reader held the shard table, so this test cannot tell a shared read from an exclusive one"
    );
    drop(guard);
    write_handle.join().unwrap();
}

/// What the shared path answers is what the ordinary path answers.
#[test]
fn a_ranked_cell_read_answers_what_the_serving_function_answers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    seed_samples(&engine, &key, 8, 16);

    for spec in [
        rank_spec(),
        WindowReduceRankSpec::new(
            WindowReduceWindow::LastN { n: 3 },
            WindowReduceScore::Lane { lane: "shown".to_string() },
            5,
        ),
    ] {
        let through_execute = engine.execute(ExecuteRequest { shard_id: 1, command: rank_command(&key, spec.clone()) });
        let CommandResponse::WindowReduceRanking { ranked } = through_execute.response else {
            panic!("not a ranking")
        };
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard");
        let (start, end) = shard.routing_range();
        let direct = crate::engine::window_reduce_rank_read::serve(
            &engine.cache,
            &engine.block_store,
            1,
            shard,
            &key,
            &spec,
            start,
            end,
            crate::engine::resolve_now_ms(),
        );
        assert!(!ranked.rows.is_empty());
        assert_eq!(format!("{ranked:?}"), format!("{direct:?}"));
    }
}

/// A spec admission refuses is refused with a status, not answered as an empty ranking.
#[test]
fn a_ranked_cell_read_with_a_spec_admission_refuses_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    seed_samples(&engine, &key, 2, 4);
    let spec = WindowReduceRankSpec::new(
        WindowReduceWindow::Between { start_ms: 10, end_ms: 5 },
        WindowReduceScore::Lane { lane: "shown".to_string() },
        5,
    );
    let response = engine.execute(ExecuteRequest { shard_id: 1, command: rank_command(&key, spec) });
    assert!(!response.status.ok, "an inverted window was served");
}
