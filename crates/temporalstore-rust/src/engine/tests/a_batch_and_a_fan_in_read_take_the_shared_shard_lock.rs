// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A batch of ranked cell reads and a fan-in read take the shared shard lock, as a single ranked
//! read does.
//!
//! Both took the shard table's WRITE lock like a write, so each queued behind every other read on
//! the shard. They now copy the entries they open out under the shared guard, release it, and read
//! and rank after.
//!
//! The guard holds one reader on the shard table and asks, from another thread, for the read. A
//! read that needs the write lock cannot be answered while it is held. THE CONTROL is a write in the
//! same state, which must NOT be answered within the wait.

#![allow(clippy::all)]
use super::*;
use super::the_window_reduce_bytes_are_counted_per_operation::{open, rank_spec, seed_samples, stored_key};
use crate::types::WindowReduceRankBatchItem;
use std::sync::{mpsc, Arc};
use std::time::Duration;

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

fn two_keys(engine: &TemporalEngine) -> (String, String) {
    let (first, second) = (stored_key(1), stored_key(2));
    seed_samples(engine, &first, 8, 16);
    seed_samples(engine, &second, 6, 16);
    (first, second)
}

fn batch(first: &str, second: &str) -> Command {
    Command::WindowReduceRankBatch {
        queries: vec![
            WindowReduceRankBatchItem { key: first.to_string(), spec: rank_spec() },
            WindowReduceRankBatchItem { key: second.to_string(), spec: rank_spec() },
        ],
    }
}

fn fan_in(first: &str, second: &str) -> Command {
    Command::WindowReduceFanInRank { keys: vec![first.to_string(), second.to_string()], spec: rank_spec() }
}

fn assert_blocked_write<T>(engine: &Arc<TemporalEngine>, key: &str, guard: std::sync::RwLockReadGuard<'_, T>) {
    let write = Command::FeatureAppend {
        key: key.to_string(),
        points: vec![FeaturePoint { timestamp_ms: 9_000, value: vec![0] }],
    };
    let (blocked, handle) = answered_within(engine, write, Duration::from_millis(500));
    assert!(
        blocked.is_none(),
        "a write was answered while a reader held the shard table, so this test cannot tell a shared read from an exclusive one"
    );
    drop(guard);
    handle.join().unwrap();
}

#[test]
fn a_batch_of_ranked_reads_is_answered_while_another_reader_holds_the_shard_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(dir.path(), None));
    let (first, second) = two_keys(&engine);
    let guard = engine.shards.read().expect("engine lock poisoned");
    let (answer, handle) = answered_within(&engine, batch(&first, &second), Duration::from_secs(5));
    let answer = answer.expect("a batch was not answered while a reader held the shard table");
    assert!(answer.status.ok, "{:?}", answer.status);
    match &answer.response {
        CommandResponse::WindowReduceRankings { rankings } => {
            assert_eq!(rankings.len(), 2);
            assert!(rankings.iter().all(|ranked| ranked.refused.is_empty() && !ranked.rows.is_empty()));
        }
        other => panic!("not rankings: {other:?}"),
    }
    handle.join().unwrap();
    assert_blocked_write(&engine, &first, guard);
}

#[test]
fn a_fan_in_read_is_answered_while_another_reader_holds_the_shard_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(dir.path(), None));
    let (first, second) = two_keys(&engine);
    let guard = engine.shards.read().expect("engine lock poisoned");
    let (answer, handle) = answered_within(&engine, fan_in(&first, &second), Duration::from_secs(5));
    let answer = answer.expect("a fan-in read was not answered while a reader held the shard table");
    assert!(answer.status.ok, "{:?}", answer.status);
    match &answer.response {
        CommandResponse::WindowReduceRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "refused: {}", ranked.refused);
            assert_eq!(ranked.keys, 2);
            assert!(!ranked.rows.is_empty());
        }
        other => panic!("not a ranking: {other:?}"),
    }
    handle.join().unwrap();
    assert_blocked_write(&engine, &first, guard);
}

/// What the shared path answers is what the serving functions answer.
#[test]
fn a_batch_and_a_fan_in_read_answer_what_the_serving_functions_answer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let (first, second) = two_keys(&engine);

    let through_batch = engine.execute(ExecuteRequest { shard_id: 1, command: batch(&first, &second) });
    let CommandResponse::WindowReduceRankings { rankings } = through_batch.response else { panic!("not rankings") };
    let through_fan_in = engine.execute(ExecuteRequest { shard_id: 1, command: fan_in(&first, &second) });
    let CommandResponse::WindowReduceRanking { ranked } = through_fan_in.response else { panic!("not a ranking") };

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard");
    let (start, end) = shard.routing_range();
    let now = crate::engine::resolve_now_ms();
    for (key, answered) in [&first, &second].into_iter().zip(&rankings) {
        let direct = crate::engine::window_reduce_rank_read::serve(
            &engine.cache,
            &engine.block_store,
            1,
            shard,
            key,
            &rank_spec(),
            start,
            end,
            now,
        );
        assert!(!answered.rows.is_empty());
        assert_eq!(format!("{answered:?}"), format!("{direct:?}"));
    }
    let direct = crate::engine::window_reduce_rank_read::serve_many(
        &engine.cache,
        &engine.block_store,
        1,
        shard,
        &[first.clone(), second.clone()],
        &rank_spec(),
        start,
        end,
        now,
    );
    assert!(!ranked.rows.is_empty());
    assert_eq!(format!("{ranked:?}"), format!("{direct:?}"));
}

/// What admission refuses is still refused with a status.
#[test]
fn a_batch_and_a_fan_in_read_admission_refuses_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let (first, _second) = two_keys(&engine);
    let empty = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::WindowReduceRankBatch { queries: Vec::new() },
    });
    assert!(!empty.status.ok, "an empty batch was served");
    let repeated = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::WindowReduceFanInRank { keys: vec![first.clone(), first], spec: rank_spec() },
    });
    assert!(!repeated.status.ok, "a key named twice was served");
}
