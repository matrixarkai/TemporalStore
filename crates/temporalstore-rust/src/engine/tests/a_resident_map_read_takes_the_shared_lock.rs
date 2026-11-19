// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A READ OF A RESIDENT MAP SHOULD NOT TAKE THE EXCLUSIVE SHARD LOCK.
//!
//! `ZSetScore`, `ZSetCard`, `ZSetScan`, `ListLen`, `SetScan`, `SetCard` and `SetIsMember` are each a
//! lookup or a bounded walk of a map the shard keeps in memory, with no block to read, and each took
//! the exclusive shard table lock, so they waited for every writer and for every other reader. They
//! now answer under the shared lock. There is no block read after the guard drops, so there is
//! nothing to pin (see `FastPathRead` for what the pin protects).
//!
//! TWO ARMS.
//!   * `each_resident_map_read_answers_while_the_shared_lock_is_held` -- the test holds the shared
//!     lock and each read must answer from another thread.
//!   * `the_fast_reads_answer_what_the_maps_hold` -- the answers against known values, over present
//!     and absent members and keys, a removed member, a cursor, a limit and a lapsed key.
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
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn seed(engine: &TemporalEngine) {
    for (member, score) in [(b"a", 1.0f64), (b"b", 2.5), (b"c", 4.0)] {
        run(engine, Command::ZSetAdd { key: "z".into(), member: member.to_vec(), score });
    }
    for member in [b"a", b"b", b"c", b"d"] {
        run(engine, Command::SetAdd { key: "s".into(), member: member.to_vec() });
    }
    for element in [b"x", b"y"] {
        run(engine, Command::ListPush { key: "l".into(), member: element.to_vec(), left: false });
    }
}

fn reads() -> Vec<(&'static str, Command, CommandResponse)> {
    vec![
        (
            "ZSetScore",
            Command::ZSetScore { key: "z".into(), member: b"b".to_vec() },
            CommandResponse::Bytes { value: Some(b"2.5".to_vec()) },
        ),
        (
            "ZSetScore of an absent member",
            Command::ZSetScore { key: "z".into(), member: b"zz".to_vec() },
            CommandResponse::Bytes { value: None },
        ),
        ("ZSetCard", Command::ZSetCard { key: "z".into() }, CommandResponse::Integer { value: 3 }),
        ("ZSetCard of an absent key", Command::ZSetCard { key: "nope".into() }, CommandResponse::Integer { value: 0 }),
        (
            "ZSetScan",
            Command::ZSetScan { key: "z".into(), after: Vec::new(), limit: 2 },
            CommandResponse::Members {
                members: vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2.5".to_vec()],
            },
        ),
        (
            "ZSetScan after a cursor",
            Command::ZSetScan { key: "z".into(), after: b"b".to_vec(), limit: 10 },
            CommandResponse::Members { members: vec![b"c".to_vec(), b"4".to_vec()] },
        ),
        ("ListLen", Command::ListLen { key: "l".into() }, CommandResponse::Integer { value: 2 }),
        ("ListLen of an absent key", Command::ListLen { key: "nope".into() }, CommandResponse::Integer { value: 0 }),
        (
            "SetScan",
            Command::SetScan { key: "s".into(), after: b"a".to_vec(), limit: 2 },
            CommandResponse::Members { members: vec![b"b".to_vec(), b"c".to_vec()] },
        ),
        ("SetCard", Command::SetCard { key: "s".into() }, CommandResponse::Integer { value: 4 }),
        (
            "SetIsMember",
            Command::SetIsMember { key: "s".into(), member: b"d".to_vec() },
            CommandResponse::Integer { value: 1 },
        ),
        (
            "SetIsMember of an absent member",
            Command::SetIsMember { key: "s".into(), member: b"zz".to_vec() },
            CommandResponse::Integer { value: 0 },
        ),
    ]
}

#[test]
fn each_resident_map_read_answers_while_the_shared_lock_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    // The ordinary path publishes the set read hint the shared path consults.
    for (_, command, _) in reads() {
        run(&engine, command);
    }
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
fn the_fast_reads_answer_what_the_maps_hold() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    for (label, command, expected) in reads() {
        assert_eq!(expected, run(&engine, command), "{label}");
    }
    // A REMOVED MEMBER, and a member re-scored.
    run(&engine, Command::SetRemove { key: "s".into(), member: b"a".to_vec() });
    assert_eq!(
        CommandResponse::Integer { value: 3 },
        run(&engine, Command::SetCard { key: "s".into() })
    );
    run(&engine, Command::ZSetAdd { key: "z".into(), member: b"a".to_vec(), score: 9.0 });
    assert_eq!(
        CommandResponse::Bytes { value: Some(b"9".to_vec()) },
        run(&engine, Command::ZSetScore { key: "z".into(), member: b"a".to_vec() })
    );
    // A LAPSED KEY answers as a missing one, through the ordinary path that drops it.
    run(&engine, Command::CommonExpire { key: "l".into(), ttl_ms: 1 });
    run(&engine, Command::CommonExpire { key: "z".into(), ttl_ms: 1 });
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(CommandResponse::Integer { value: 0 }, run(&engine, Command::ListLen { key: "l".into() }));
    assert_eq!(CommandResponse::Integer { value: 0 }, run(&engine, Command::ZSetCard { key: "z".into() }));
}
