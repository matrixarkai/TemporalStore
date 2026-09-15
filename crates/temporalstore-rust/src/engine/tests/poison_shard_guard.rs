// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one panic under the shard-table guard costs every later caller.
//!
//! `RwLock` marks itself POISONED when a thread unwinds while holding it. Every later
//! `.read()`/`.write()` on that lock returns `Err`, and the shard table's acquisitions all spell
//! that `.expect("engine lock poisoned")`. So the question this module answers with a number is
//! not "can the lock be poisoned" -- it can -- but what the NEXT ordinary request observes once
//! it has been, and for how long.
#![allow(clippy::all)]
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Requests driven in each arm. Larger than one so "the first call after the poison fails" and
/// "every call after the poison fails" are distinguishable outcomes rather than the same one.
const PROBES: usize = 3;

/// A loaded shard holding one string key.
fn engine_with_one_key(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    let written = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(written.status.ok, "fixture write: {:?}", written.status);
    engine
}

/// Drive one `StringGet` down the plain serving route and say what the CALLER got.
///
/// Three outcomes are distinguished on purpose, because they are three different severities:
/// a clean error the client can retry, a wrong-but-successful answer, and an unwinding panic
/// that never returns a response at all.
enum Observed {
    Served(Option<Vec<u8>>),
    CleanError(String),
    Panicked(String),
}

fn observe_get(engine: &TemporalEngine) -> Observed {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        })
    }));
    match outcome {
        Err(payload) => Observed::Panicked(panic_message(payload)),
        Ok(response) if !response.status.ok => Observed::CleanError(response.status.code.clone()),
        Ok(response) => match response.response {
            CommandResponse::Bytes { value } => Observed::Served(value),
            other => panic!("expected Bytes, got {other:?}"),
        },
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Poison the shard table the way production would: a thread unwinds while holding the guard.
///
/// Nothing here reaches into the lock's internals -- the guard is taken through the same field
/// every acquisition uses, and the panic is a real unwind on a real thread, so the poison flag is
/// set by `RwLock` itself rather than simulated.
fn poison_shard_table(engine: &TemporalEngine) {
    let handle = {
        let engine = engine.clone();
        std::thread::spawn(move || {
            let _guard = engine.shards.write().expect("engine lock poisoned");
            // Deliberate: this is the event being priced. libtest prints the unwind below; a
            // "panicked at ... poisoning the shard table on purpose" line in this test's output
            // is the fixture working, not a failure.
            panic!("poisoning the shard table on purpose");
        })
    };
    assert!(
        handle.join().is_err(),
        "the fixture thread must have UNWOUND; if it returned Ok nothing was poisoned and every \
         number below would be vacuous"
    );
}

/// One panic under the shard guard turns every later request on that process into a panic.
///
/// FOUR claims, asserted apart, because a single end-to-end assertion passes when two of them
/// are wrong in opposite directions:
///
///   1. BEFORE  -- the fixture actually serves. Without this the "after" arm could be measuring
///                 a shard that never worked.
///   2. POISON  -- the lock itself reports poisoned on BOTH `read()` and `write()`. This is the
///                 mechanism; if it does not hold, arm 3 is measuring something else.
///   3. AFTER   -- every one of `PROBES` ordinary requests panics, and the panic carries the
///                 in-tree message, so the failure is the shard-table `expect` and not an
///                 unrelated one.
///   4. FOREVER -- there is no recovery: the poison is still set after the requests, so this is
///                 a permanent property of the process, not a transient one.
#[test]
fn a_panic_under_the_shard_guard_makes_every_later_request_panic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_one_key(dir.path());

    // --- 1. BEFORE ---------------------------------------------------------------------------
    let mut served_before = 0usize;
    for _ in 0..PROBES {
        match observe_get(&engine) {
            Observed::Served(Some(value)) => {
                assert_eq!(value, b"v".to_vec());
                served_before += 1;
            }
            Observed::Served(None) => panic!("fixture key missing before the poison"),
            Observed::CleanError(code) => panic!("fixture errored before the poison: {code}"),
            Observed::Panicked(message) => panic!("fixture panicked before the poison: {message}"),
        }
    }
    assert!(PROBES > 0, "VACUITY FLOOR: zero probes proves nothing");
    assert_eq!(
        served_before, PROBES,
        "before the poison {served_before} of {PROBES} requests were served"
    );

    // --- 2. POISON ---------------------------------------------------------------------------
    poison_shard_table(&engine);
    assert!(
        engine.shards.read().is_err(),
        "the shard table's READ side is not poisoned"
    );
    assert!(
        engine.shards.write().is_err(),
        "the shard table's WRITE side is not poisoned"
    );

    // --- 3. AFTER ----------------------------------------------------------------------------
    let mut panicked_after = 0usize;
    let mut clean_error_after = 0usize;
    let mut served_after = 0usize;
    let mut messages: Vec<String> = Vec::new();
    for _ in 0..PROBES {
        match observe_get(&engine) {
            Observed::Panicked(message) => {
                messages.push(message);
                panicked_after += 1;
            }
            Observed::CleanError(_) => clean_error_after += 1,
            Observed::Served(_) => served_after += 1,
        }
    }
    println!(
        "after one panic under the shard guard, of {PROBES} ordinary StringGet requests: \
         {panicked_after} panicked, {clean_error_after} returned a clean error, \
         {served_after} were served"
    );
    assert_eq!(panicked_after, PROBES, "panicking requests");
    assert_eq!(clean_error_after, 0, "clean-error requests");
    assert_eq!(served_after, 0, "served requests");
    assert!(
        messages
            .iter()
            .all(|message| message.contains("engine lock poisoned")),
        "the panics must come from the shard-table expect, got {messages:?}"
    );

    // --- 4. FOREVER --------------------------------------------------------------------------
    assert!(
        engine.shards.read().is_err(),
        "nothing on the request path clears the poison, so it must still be set"
    );
}


/// The serving READ fast path is poisoned by the same one panic, at a different acquisition.
///
/// `execute` takes the shard table's WRITE lock even for a read; `execute_durable` declines that
/// and takes the READ lock through the marked accessor. They are two different acquisitions of
/// the same lock, so a claim about one is not a claim about the other -- and a `RwLock`'s poison
/// closes BOTH sides, which is the whole reason the read path is not the cheaper case here.
///
/// Asserted in two halves for that reason: the read route served before, and the read route
/// panicked after, with the panic carrying the shard-table message rather than some other one.
#[test]
fn the_serving_read_fast_path_is_poisoned_by_the_same_panic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_one_key(dir.path());

    let durable_get = |engine: &TemporalEngine| {
        catch_unwind(AssertUnwindSafe(|| {
            engine.execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            })
        }))
    };

    let mut served_before = 0usize;
    for _ in 0..PROBES {
        let response = durable_get(&engine).expect("the read route must not panic before");
        assert!(response.status.ok, "before: {:?}", response.status);
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        served_before += 1;
    }
    assert_eq!(served_before, PROBES, "read route served before the poison");

    poison_shard_table(&engine);

    let mut panicked_after = 0usize;
    let mut messages: Vec<String> = Vec::new();
    for _ in 0..PROBES {
        match durable_get(&engine) {
            Err(payload) => {
                messages.push(panic_message(payload));
                panicked_after += 1;
            }
            Ok(response) => panic!(
                "the read route returned instead of panicking: {:?}",
                response.status
            ),
        }
    }
    println!(
        "read fast path after the poison: {panicked_after} of {PROBES} requests panicked"
    );
    assert_eq!(panicked_after, PROBES, "read route panics after the poison");
    assert!(
        messages
            .iter()
            .all(|message| message.contains("engine lock poisoned")),
        "the read route's panics must come from the shard-table expect, got {messages:?}"
    );
}
