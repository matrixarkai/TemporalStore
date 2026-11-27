// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A RANGE OF A SORTED SET, THE MEMBERS OF A SET AND A RANGE OF A LIST TAKE THE SHARED SHARD LOCK.
//!
//! `ZSetRange` and `SetMembers` answer from a map the shard keeps in memory, so they read no block
//! and have nothing to pin. `ListRange` reads one block per element it returns, and a block that
//! still lives only in the write-ahead log can be overwritten the moment the guard drops, so its
//! addresses are PINNED under the guard exactly as a hash read's are (see `FastPathRead` and
//! `wal_read_snapshot_tests`), and a block that cannot be read declines the fast path so the ordinary
//! path answers.
//!
//! THREE ARMS.
//!   * `each_read_answers_while_the_shared_lock_is_held` -- the test holds the shared lock and each
//!     read must answer from another thread.
//!   * `the_fast_reads_answer_what_the_ordinary_arms_answered` -- the answers against known values:
//!     windows, negative indices, both directions, a set with and without members, a list with a hole
//!     left by a removal, an absent key and a lapsed key.
//!   * `a_list_range_keeps_its_snapshot_when_its_elements_are_overwritten_after_the_plan` -- the pin:
//!     with the elements still in the log, every element is overwritten between the plan and the
//!     read, and the read must still return the values it planned for, whole.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
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
    let outcome = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn members(response: CommandResponse) -> Vec<Vec<u8>> {
    match response {
        CommandResponse::Members { members } => members,
        other => panic!("members expected: {other:?}"),
    }
}

fn seed(engine: &TemporalEngine) {
    for (member, score) in [(b"a", 1.0f64), (b"b", 2.5), (b"c", 4.0), (b"d", 4.0), (b"e", 7.0)] {
        run(engine, Command::ZSetAdd { key: "z".into(), member: member.to_vec(), score });
    }
    for member in [b"d", b"b", b"a", b"c"] {
        run(engine, Command::SetAdd { key: "s".into(), member: member.to_vec() });
    }
    for element in [b"w", b"x", b"y", b"z"] {
        run(engine, Command::ListPush { key: "l".into(), member: element.to_vec(), left: false });
    }
}

fn v(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn reads() -> Vec<(&'static str, Command, CommandResponse)> {
    vec![
        (
            "ZSetRange, the first two",
            Command::ZSetRange { key: "z".into(), start: 0, stop: 1, rev: false },
            CommandResponse::Members { members: vec![v("a"), v("1"), v("b"), v("2.5")] },
        ),
        (
            "ZSetRange, from the high end",
            Command::ZSetRange { key: "z".into(), start: 0, stop: 2, rev: true },
            CommandResponse::Members { members: vec![v("e"), v("7"), v("d"), v("4"), v("c"), v("4")] },
        ),
        (
            "ZSetRange, negative indices",
            Command::ZSetRange { key: "z".into(), start: -2, stop: -1, rev: false },
            CommandResponse::Members { members: vec![v("d"), v("4"), v("e"), v("7")] },
        ),
        (
            "ZSetRange of an absent key",
            Command::ZSetRange { key: "nope".into(), start: 0, stop: -1, rev: false },
            CommandResponse::Members { members: Vec::new() },
        ),
        (
            "SetMembers",
            Command::SetMembers { key: "s".into() },
            CommandResponse::Members { members: vec![v("a"), v("b"), v("c"), v("d")] },
        ),
        (
            "SetMembers of an absent key",
            Command::SetMembers { key: "nope".into() },
            CommandResponse::Members { members: Vec::new() },
        ),
        (
            "ListRange, the middle",
            Command::ListRange { key: "l".into(), start: 1, stop: 2 },
            CommandResponse::Members { members: vec![v("x"), v("y")] },
        ),
        (
            "ListRange, the whole list",
            Command::ListRange { key: "l".into(), start: 0, stop: -1 },
            CommandResponse::Members { members: vec![v("w"), v("x"), v("y"), v("z")] },
        ),
        (
            "ListRange of an absent key",
            Command::ListRange { key: "nope".into(), start: 0, stop: -1 },
            CommandResponse::Members { members: Vec::new() },
        ),
    ]
}

#[test]
fn each_read_answers_while_the_shared_lock_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    // The ordinary path publishes the set read hint the shared path consults.
    for (_, command, _) in reads() {
        run(&engine, command);
    }
    for (label, command, expected) in reads() {
        // A set the shard does not hold answers through the fold over the blocks, which is the
        // ordinary path and takes the exclusive lock; the others answer under the shared one.
        if label == "SetMembers of an absent key" {
            continue;
        }
        let (got, worker) = engine.shards.with_state(1, |owner| {
            assert!(owner.is_some());
            let (sender, receiver) = mpsc::channel();
            let worker = {
                let engine = Arc::clone(&engine);
                std::thread::spawn(move || {
                    let _ = sender.send(run(&engine, command));
                })
            };
            let got = receiver.recv_timeout(Duration::from_secs(5));
            (got, worker)
        });
        worker.join().expect("the read thread");
        assert_eq!(
            Ok(expected),
            got.map_err(|_| "no answer while the shared lock was held"),
            "{label}"
        );
    }
}

#[test]
fn the_fast_reads_answer_what_the_ordinary_arms_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    for (label, command, expected) in reads() {
        assert_eq!(expected, run(&engine, command), "{label}");
    }
    // A removed member leaves the set and the sorted set, and a re-score moves a member.
    run(&engine, Command::SetRemove { key: "s".into(), member: v("a") });
    assert_eq!(vec![v("b"), v("c"), v("d")], members(run(&engine, Command::SetMembers { key: "s".into() })));
    run(&engine, Command::ZSetAdd { key: "z".into(), member: v("a"), score: 9.0 });
    assert_eq!(
        vec![v("e"), v("7"), v("a"), v("9")],
        members(run(&engine, Command::ZSetRange { key: "z".into(), start: -2, stop: -1, rev: false }))
    );
    // A removal from the middle of a list leaves a hole in its positions; a range still answers.
    run(&engine, Command::ListRemove { key: "l".into(), count: 1, member: v("x") });
    assert_eq!(
        vec![v("w"), v("y"), v("z")],
        members(run(&engine, Command::ListRange { key: "l".into(), start: 0, stop: -1 }))
    );
    assert_eq!(
        vec![v("y")],
        members(run(&engine, Command::ListRange { key: "l".into(), start: 1, stop: 1 }))
    );
    // A lapsed key answers as a missing one, through the ordinary path that drops it.
    run(&engine, Command::CommonExpire { key: "l".into(), ttl_ms: 1 });
    run(&engine, Command::CommonExpire { key: "z".into(), ttl_ms: 1 });
    run(&engine, Command::CommonExpire { key: "s".into(), ttl_ms: 1 });
    std::thread::sleep(Duration::from_millis(30));
    assert!(members(run(&engine, Command::ListRange { key: "l".into(), start: 0, stop: -1 })).is_empty());
    assert!(members(run(&engine, Command::ZSetRange { key: "z".into(), start: 0, stop: -1, rev: false })).is_empty());
    assert!(members(run(&engine, Command::SetMembers { key: "s".into() })).is_empty());
}

#[test]
fn a_list_range_keeps_its_snapshot_when_its_elements_are_overwritten_after_the_plan() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine
        .set_config(crate::SetConfigRequest {
            shard_id: 1,
            config: crate::Config { version: 2, async_storage: true, ..Default::default() },
        })
        .ok);
    for (n, byte) in [1u8, 2, 3, 4].iter().enumerate() {
        let _ = n;
        run(&engine, Command::ListPush { key: "l".into(), member: vec![*byte; 48], left: false });
    }
    engine.cache.invalidate_shard(1).unwrap();
    let expected = CommandResponse::Members { members: (1..=4u8).map(|b| vec![b; 48]).collect() };
    let writer = engine.clone();
    crate::engine::AFTER_FAST_READ_PLAN.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(move || {
            assert!(
                !crate::engine::shard_write_guard::any_shard_guard_held(),
                "payload loading must not retain the index guard"
            );
            std::thread::spawn(move || {
                for index in 0..4i64 {
                    run(&writer, Command::ListSet { key: "l".into(), index, member: vec![9u8; 48] });
                }
                writer.cache.invalidate_shard(1).unwrap();
            })
            .join()
            .unwrap();
        }));
    });
    assert_eq!(expected, run(&engine, Command::ListRange { key: "l".into(), start: 0, stop: -1 }));
    crate::engine::AFTER_FAST_READ_PLAN.with(|slot| assert!(slot.borrow().is_none()));
    // And the overwrite did land: the next read sees the new values.
    assert_eq!(
        CommandResponse::Members { members: vec![vec![9u8; 48]; 4] },
        run(&engine, Command::ListRange { key: "l".into(), start: 0, stop: -1 })
    );
}
