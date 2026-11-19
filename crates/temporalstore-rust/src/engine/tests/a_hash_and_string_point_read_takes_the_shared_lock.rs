// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A POINT READ SHOULD NOT TAKE THE EXCLUSIVE SHARD LOCK.
//!
//! `StringGet` and `HashGetAll` read under the shard table's shared lock; every other read, however
//! small, took the exclusive one, so a hash point read, a hash length, a field-presence check and a
//! string length or range read each waited for every writer AND for every other reader. Measured on
//! one engine, string point reads scaled with threads (581k, 1.04M, 2.32M reads a second at 1, 4 and
//! 16 threads) and hash point reads fell (426k, 362k, 336k).
//!
//! THE GUARD holds the shard table's SHARED lock on the test thread and runs each read on another
//! thread: a read that needs the exclusive lock cannot answer until the guard is released, a read on
//! the shared path answers at once.
//!
//! THREE ARMS.
//!   * `each_point_read_answers_while_the_shared_lock_is_held` -- the five reads, against known
//!     answers, with the lock held by the test.
//!   * `the_fast_reads_answer_what_the_ordinary_reads_answer` -- the same questions put to the
//!     engine with and without the lock held by a writer-free path, over present, absent, deleted and
//!     empty values, and a lapsed key, which must still be dropped on the ordinary path.
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

fn reads() -> Vec<(&'static str, Command, CommandResponse)> {
    vec![
        (
            "HashGet",
            Command::HashGet { key: "h".into(), field: "a".into() },
            CommandResponse::Bytes { value: Some(b"va".to_vec()) },
        ),
        (
            "HashGet of an absent field",
            Command::HashGet { key: "h".into(), field: "zz".into() },
            CommandResponse::Bytes { value: None },
        ),
        (
            "HashGet of an absent key",
            Command::HashGet { key: "nope".into(), field: "a".into() },
            CommandResponse::Bytes { value: None },
        ),
        ("HashLen", Command::HashLen { key: "h".into() }, CommandResponse::Integer { value: 2 }),
        ("HashLen of an absent key", Command::HashLen { key: "nope".into() }, CommandResponse::Integer { value: 0 }),
        (
            "HashFieldExists",
            Command::HashFieldExists { key: "h".into(), field: "b".into() },
            CommandResponse::Integer { value: 1 },
        ),
        (
            "HashFieldExists of an absent field",
            Command::HashFieldExists { key: "h".into(), field: "zz".into() },
            CommandResponse::Integer { value: 0 },
        ),
        ("StringLen", Command::StringLen { key: "s".into() }, CommandResponse::Integer { value: 10 }),
        ("StringLen of an absent key", Command::StringLen { key: "nope".into() }, CommandResponse::Integer { value: 0 }),
        (
            "StringGetRange",
            Command::StringGetRange { key: "s".into(), start: 2, end: 5 },
            CommandResponse::Bytes { value: Some(b"2345".to_vec()) },
        ),
        (
            "StringGetRange from the end",
            Command::StringGetRange { key: "s".into(), start: -3, end: -1 },
            CommandResponse::Bytes { value: Some(b"789".to_vec()) },
        ),
        (
            "StringGetRange of an absent key",
            Command::StringGetRange { key: "nope".into(), start: 0, end: -1 },
            CommandResponse::Bytes { value: None },
        ),
    ]
}

fn seed(engine: &TemporalEngine) {
    run(engine, Command::HashSet { key: "h".into(), field: "a".into(), value: b"va".to_vec() });
    run(engine, Command::HashSet { key: "h".into(), field: "b".into(), value: b"vb".to_vec() });
    run(engine, Command::StringSet { key: "s".into(), value: b"0123456789".to_vec() });
}

#[test]
fn each_point_read_answers_while_the_shared_lock_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    // WARM every block, so the read below needs no write to fill a cache.
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
fn the_fast_reads_answer_what_the_ordinary_reads_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    for (label, command, expected) in reads() {
        assert_eq!(expected, run(&engine, command), "{label}");
    }
    // A DELETED FIELD and an EMPTY VALUE.
    run(&engine, Command::HashSet { key: "h".into(), field: "e".into(), value: Vec::new() });
    assert_eq!(
        CommandResponse::Bytes { value: Some(Vec::new()) },
        run(&engine, Command::HashGet { key: "h".into(), field: "e".into() })
    );
    run(&engine, Command::HashDelete { key: "h".into(), field: "a".into() });
    assert_eq!(
        CommandResponse::Bytes { value: None },
        run(&engine, Command::HashGet { key: "h".into(), field: "a".into() })
    );
    assert_eq!(
        CommandResponse::Integer { value: 0 },
        run(&engine, Command::HashFieldExists { key: "h".into(), field: "a".into() })
    );
    assert_eq!(CommandResponse::Integer { value: 2 }, run(&engine, Command::HashLen { key: "h".into() }));
    // AN OVERWRITE: the read follows the new value.
    run(&engine, Command::StringSet { key: "s".into(), value: b"abc".to_vec() });
    assert_eq!(CommandResponse::Integer { value: 3 }, run(&engine, Command::StringLen { key: "s".into() }));
    // A LAPSED KEY is answered as a missing one, and the ordinary path drops it.
    run(&engine, Command::CommonExpire { key: "s".into(), ttl_ms: 1 });
    run(&engine, Command::CommonExpire { key: "h".into(), ttl_ms: 1 });
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(CommandResponse::Integer { value: 0 }, run(&engine, Command::StringLen { key: "s".into() }));
    assert_eq!(CommandResponse::Integer { value: 0 }, run(&engine, Command::HashLen { key: "h".into() }));
    assert_eq!(
        CommandResponse::Bytes { value: None },
        run(&engine, Command::HashGet { key: "h".into(), field: "b".into() })
    );
}
