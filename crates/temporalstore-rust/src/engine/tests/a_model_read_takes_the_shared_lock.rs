// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A COUNTER, DISTINCT, SELECTION OR SEEN READ SHOULD NOT TAKE THE EXCLUSIVE SHARD LOCK.
//!
//! `StringGet`, the hash point reads and the feature range reads answer under the shard table's
//! SHARED lock. The counter window reads (`ControlStateCount`, `ControlStateQuery` including the
//! distinct `change` count), the selection read and the seen-set cardinality answer from resident
//! maps with nothing to read afterwards, yet each took the EXCLUSIVE lock, so a read waited for every
//! writer and for every other reader. Measured on one engine, a counter window read fell as threads
//! were added (226k, 85k and 66k reads a second at 1, 4 and 16 threads) where the shared path's
//! reads scale.
//!
//! The fast path now plans them under the shared guard, from the same maps and with the same
//! reductions as the ordinary arms, and declines wherever the ordinary arm does something a reader
//! may not: a lapsed key (which the ordinary path drops), a family key that lapsed, and a shard that
//! has the counter rollup switched on.
//!
//! THREE GUARDS.
//!   * `each_model_read_answers_while_the_shared_lock_is_held` holds the shard table's shared lock on
//!     the test thread and requires every read to answer at once on another thread.
//!   * `the_fast_reads_answer_what_the_ordinary_reads_answer` runs the same questions with the lock
//!     NOT held (the fast path) against a model of the answers, over present, absent, empty and lapsed
//!     keys, every verb, a window past the series and the rollup arm.
//!   * a lapsed key must still be dropped: the read answers as a missing one and a later read agrees.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{CommandResponse, ControlStateSelectionType};
use std::sync::{mpsc, Arc};
use std::time::Duration;

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

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn integer(value: i64) -> CommandResponse {
    CommandResponse::Integer { value }
}

fn query(key: &str, start_ms: u64, end_ms: u64, aggregator: &str) -> Command {
    Command::ControlStateQuery {
        key: key.into(),
        start_ms,
        end_ms,
        aggregator: aggregator.into(),
    }
}

/// The reads, each against the answer the seeded state fixes.
fn reads() -> Vec<(&'static str, Command, CommandResponse)> {
    vec![
        ("count", Command::ControlStateCount { key: "c".into(), start_ms: 0, end_ms: u64::MAX }, integer(15)),
        ("count of a window", Command::ControlStateCount { key: "c".into(), start_ms: 1_010, end_ms: 1_020 }, integer(10)),
        ("count of an absent key", Command::ControlStateCount { key: "nope".into(), start_ms: 0, end_ms: u64::MAX }, integer(0)),
        ("sum", query("c", 0, u64::MAX, "sum"), integer(15)),
        ("min", query("c", 0, u64::MAX, "min"), integer(3)),
        ("max", query("c", 0, u64::MAX, "MAX"), integer(7)),
        ("first", query("c", 0, u64::MAX, "first"), integer(5)),
        ("last", query("c", 0, u64::MAX, "last"), integer(3)),
        ("events", query("c", 0, u64::MAX, "events"), integer(3)),
        ("an unknown verb sums", query("c", 0, u64::MAX, "bogus"), integer(15)),
        ("a window past the series", query("c", 5_000, 6_000, "sum"), integer(0)),
        ("the distinct count", query("d", 0, u64::MAX, "change"), integer(3)),
        ("a distinct window", query("d", 1_000, 1_010, "change"), integer(2)),
        ("the distinct count of an absent key", query("nope", 0, u64::MAX, "change"), integer(0)),
        (
            "the selection",
            Command::ControlStateSelectionQuery { key: "l".into() },
            CommandResponse::Bytes { value: Some(b"second".to_vec()) },
        ),
        (
            "the selection of an absent key",
            Command::ControlStateSelectionQuery { key: "nope".into() },
            CommandResponse::Bytes { value: None },
        ),
        ("the seen cardinality", Command::SeenCard { key: "s".into() }, integer(2)),
        ("the seen cardinality of an absent key", Command::SeenCard { key: "nope".into() }, integer(0)),
    ]
}

fn seed(engine: &TemporalEngine) {
    for (at, amount) in [(1_000u64, 5i64), (1_010, 7), (1_020, 3)] {
        run(engine, Command::ControlStateIncrement { key: "c".into(), timestamp_ms: at, amount });
    }
    for (at, member) in [(1_000u64, "a"), (1_000, "b"), (1_010, "b"), (1_020, "c")] {
        run(
            engine,
            Command::ControlStateChangeAdd {
                key: "d".into(),
                timestamp_ms: at,
                value: member.as_bytes().to_vec(),
                precision_ms: None,
                ttl_ms: None,
            },
        );
    }
    for (at, value, kind) in [(10u64, "first", ControlStateSelectionType::Last), (20, "second", ControlStateSelectionType::Last)] {
        run(
            engine,
            Command::ControlStateSelectionSet {
                key: "l".into(),
                value: value.as_bytes().to_vec(),
                occur_time_ms: at,
                ttl_ms: 0,
                selection_type: kind,
            },
        );
    }
    for member in ["x", "y", "x"] {
        run(engine, Command::SeenCheck { key: "s".into(), member: member.as_bytes().to_vec(), window_ms: 3_600_000 });
    }
}

#[test]
fn each_model_read_answers_while_the_shared_lock_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    let mut answered = 0usize;
    for (label, command, expected) in reads() {
        let held = engine.shards.read().expect("engine lock poisoned");
        let (sender, receiver) = mpsc::channel();
        let worker = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                let _ = sender.send(run(&engine, command));
            })
        };
        let got = receiver.recv_timeout(Duration::from_secs(5));
        drop(held);
        worker.join().expect("the read thread");
        assert_eq!(
            Ok(expected),
            got.map_err(|_| "no answer while the shared lock was held"),
            "{label}"
        );
        answered += 1;
    }
    assert_eq!(reads().len(), answered);
}

#[test]
fn the_fast_reads_answer_what_the_ordinary_reads_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    for (label, command, expected) in reads() {
        assert_eq!(expected, run(&engine, command), "{label}");
    }
    // A WRITE THEN A READ: the read follows it.
    run(&engine, Command::ControlStateIncrement { key: "c".into(), timestamp_ms: 1_030, amount: 100 });
    assert_eq!(integer(115), run(&engine, query("c", 0, u64::MAX, "sum")));
    assert_eq!(integer(4), run(&engine, query("c", 0, u64::MAX, "events")));
    run(&engine, Command::SeenCheck { key: "s".into(), member: b"z".to_vec(), window_ms: 3_600_000 });
    assert_eq!(integer(3), run(&engine, Command::SeenCard { key: "s".into() }));
    run(
        &engine,
        Command::ControlStateSelectionSet {
            key: "l".into(),
            value: b"third".to_vec(),
            occur_time_ms: 30,
            ttl_ms: 0,
            selection_type: ControlStateSelectionType::Last,
        },
    );
    assert_eq!(
        CommandResponse::Bytes { value: Some(b"third".to_vec()) },
        run(&engine, Command::ControlStateSelectionQuery { key: "l".into() })
    );
    // A LAPSED KEY is answered as a missing one, and the ordinary path drops it for good.
    for key in ["c", "d", "l", "s"] {
        run(&engine, Command::CommonExpire { key: key.into(), ttl_ms: 1 });
    }
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(integer(0), run(&engine, query("c", 0, u64::MAX, "sum")));
    assert_eq!(integer(0), run(&engine, Command::ControlStateCount { key: "c".into(), start_ms: 0, end_ms: u64::MAX }));
    assert_eq!(integer(0), run(&engine, query("d", 0, u64::MAX, "change")));
    assert_eq!(
        CommandResponse::Bytes { value: None },
        run(&engine, Command::ControlStateSelectionQuery { key: "l".into() })
    );
    assert_eq!(integer(0), run(&engine, Command::SeenCard { key: "s".into() }));
    // ... and a second read agrees, so nothing the first answered was a view of a dropped key.
    assert_eq!(integer(0), run(&engine, query("c", 0, u64::MAX, "sum")));
}

#[test]
fn a_lapsed_key_declines_to_the_ordinary_path_which_drops_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    let deadlines = |engine: &TemporalEngine| -> Vec<String> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard");
        let mut keys: Vec<String> = shard.expires_at_ms().keys().map(|key| key.to_string()).collect();
        keys.sort();
        keys
    };
    run(&engine, Command::CommonExpire { key: "c".into(), ttl_ms: 1 });
    run(&engine, Command::CommonExpire { key: "l".into(), ttl_ms: 1 });
    assert_eq!(deadlines(&engine), vec!["c".to_string(), "l".to_string()]);
    std::thread::sleep(Duration::from_millis(30));
    // THE FAST PATH NEVER MUTATES, so a deadline that is gone after the read means the ordinary
    // path answered it and dropped the key: the read declined.
    assert_eq!(integer(0), run(&engine, query("c", 0, u64::MAX, "sum")));
    assert_eq!(deadlines(&engine), vec!["l".to_string()], "the counter read left its lapsed key");
    assert_eq!(
        CommandResponse::Bytes { value: None },
        run(&engine, Command::ControlStateSelectionQuery { key: "l".into() })
    );
    assert!(deadlines(&engine).is_empty(), "the selection read left its lapsed key");
    // A key that never had a deadline is not dropped by anything, and a live deadline does not make
    // the read decline for good: the unexpired keys still answer.
    run(&engine, Command::CommonExpire { key: "d".into(), ttl_ms: 3_600_000 });
    assert_eq!(integer(3), run(&engine, query("d", 0, u64::MAX, "change")));
    assert_eq!(deadlines(&engine), vec!["d".to_string()]);
}

#[test]
fn a_read_that_fetches_no_blocks_pins_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    // The #3230 pin match maps a read that fetches no blocks to no pin: neither a location pin on the
    // log nor a slab hold, because nothing is read after the guard drops.
    let answer = FastPathRead::Answer { value: 3, routing_bucket: 0 };
    assert!(answer.pin_log_locations(&engine.block_store, 1).is_none());
    assert!(answer.pin_feature_slabs(&engine.block_store).is_none());
    let value = FastPathRead::Value { value: Some(b"x".to_vec()), routing_bucket: 0 };
    assert!(value.pin_log_locations(&engine.block_store, 1).is_none());
    assert!(value.pin_feature_slabs(&engine.block_store).is_none());
}
