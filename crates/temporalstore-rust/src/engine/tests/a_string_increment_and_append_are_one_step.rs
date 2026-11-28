// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `INCRBY` and `APPEND` on a string are ONE command on the shard, not a read and a write.
//!
//! The RESP door used to build both out of `StringGet` followed by `StringSet`. Each of those is
//! atomic; the pair is not, so two clients incrementing one counter could both read 41 and both
//! write 42. The counter then falls behind by one for every pair that overlapped, with no error
//! anywhere. `StringIncrBy` and `StringAppend` do the whole step while the shard is held.
//!
//! WHAT EACH TEST ASSERTS, AND WHY IT CANNOT PASS ON THE OLD SHAPE.
//!
//! - Contention: every answer a writer gets back is distinct and the final value equals the
//!   number of writes. A lost update shows up as the same answer handed to two writers, which the
//!   old read-then-write pair does under overlap and this path cannot.
//! - The limit: the largest sum is ACCEPTED and one past it is REFUSED with the stored value left
//!   alone. Both sides are asserted, because a check that refuses everything passes the second
//!   half and a check that refuses nothing passes the first.
//! - The deadline: the key's countdown is kept, and a restart still has the value.

#![allow(clippy::all)]
use super::*;

use crate::redis::{execute_redis_command, RespValue};
use std::collections::BTreeSet;
use std::sync::Arc;

fn resp(engine: &TemporalEngine, args: &[&str]) -> RespValue {
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.as_bytes().to_vec()).collect();
    execute_redis_command(argv, 1, |command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message.clone())
        }
    })
}

fn run(engine: &TemporalEngine, command: Command) -> ExecuteResponse {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    })
}

fn get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match resp(engine, &["GET", key]) {
        RespValue::Bulk(value) => value,
        other => panic!("GET {key} answered {other:?}"),
    }
}

fn pttl(engine: &TemporalEngine, key: &str) -> i64 {
    match resp(engine, &["PTTL", key]) {
        RespValue::Integer(value) => value,
        other => panic!("PTTL {key} answered {other:?}"),
    }
}

const WRITERS: usize = 8;
const WRITES_EACH: usize = 250;

/// Every thread does `WRITES_EACH` steps on ONE key and keeps every answer. If any two overlapped
/// on a read-then-write, two answers are equal and the total is short.
fn answers_under_contention(engine: &Arc<TemporalEngine>, args: [&'static str; 3]) -> Vec<i64> {
    let mut all = Vec::new();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..WRITERS)
            .map(|_| {
                let engine = Arc::clone(engine);
                scope.spawn(move || {
                    (0..WRITES_EACH)
                        .map(|_| match resp(&engine, &args) {
                            RespValue::Integer(value) => value,
                            other => panic!("{args:?} answered {other:?}"),
                        })
                        .collect::<Vec<i64>>()
                })
            })
            .collect();
        for handle in handles {
            all.extend(handle.join().expect("a writer panicked"));
        }
    });
    all
}

#[test]
fn concurrent_increments_of_one_counter_lose_no_update() {
    let engine = Arc::new(TemporalEngine::default());
    engine.load_shard(1);

    let answers = answers_under_contention(&engine, ["INCRBY", "ctr:contended", "1"]);
    let total = (WRITERS * WRITES_EACH) as i64;

    // DENOMINATOR: the run really made this many steps, or "no duplicates" is about nothing.
    assert_eq!(answers.len() as i64, total);
    let distinct: BTreeSet<i64> = answers.iter().copied().collect();
    assert_eq!(
        distinct.len() as i64,
        total,
        "two writers were handed the same answer: one of their increments was lost"
    );
    assert_eq!(distinct.first().copied(), Some(1));
    assert_eq!(distinct.last().copied(), Some(total));
    assert_eq!(get(&engine, "ctr:contended"), Some(total.to_string().into_bytes()));
}

#[test]
fn concurrent_appends_to_one_string_drop_no_bytes() {
    let engine = Arc::new(TemporalEngine::default());
    engine.load_shard(1);

    let lengths = answers_under_contention(&engine, ["APPEND", "str:contended", "x"]);
    let total = (WRITERS * WRITES_EACH) as i64;

    assert_eq!(lengths.len() as i64, total);
    let distinct: BTreeSet<i64> = lengths.iter().copied().collect();
    assert_eq!(
        distinct.len() as i64,
        total,
        "two appends were told the same length: one of them wrote over the other"
    );
    assert_eq!(distinct.last().copied(), Some(total));
    assert_eq!(
        get(&engine, "str:contended").map(|value| value.len() as i64),
        Some(total)
    );
}

#[test]
fn the_largest_sum_is_accepted_and_one_past_it_is_refused_with_the_value_left_alone() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- the top of the range: i64::MAX - 5, plus 5, is exactly i64::MAX ---------------
    assert_eq!(
        resp(&engine, &["SET", "lim:top", &(i64::MAX - 5).to_string()]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        resp(&engine, &["INCRBY", "lim:top", "5"]),
        RespValue::Integer(i64::MAX),
        "a sum that fits must be accepted at the limit"
    );
    // ---- one past it ----------------------------------------------------------------
    assert_eq!(
        resp(&engine, &["INCRBY", "lim:top", "1"]),
        RespValue::Error("ERR increment or decrement would overflow".to_string()),
    );
    assert_eq!(
        get(&engine, "lim:top"),
        Some(i64::MAX.to_string().into_bytes()),
        "a refused increment must leave the stored value alone"
    );

    // ---- the bottom of the range -------------------------------------------------------
    assert_eq!(
        resp(&engine, &["SET", "lim:bottom", &(i64::MIN + 3).to_string()]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        resp(&engine, &["INCRBY", "lim:bottom", "-3"]),
        RespValue::Integer(i64::MIN)
    );
    assert_eq!(
        resp(&engine, &["INCRBY", "lim:bottom", "-1"]),
        RespValue::Error("ERR increment or decrement would overflow".to_string()),
    );

    // ---- through the command itself: the refusal carries its own code ----------------
    let refused = run(
        &engine,
        Command::StringIncrBy {
            key: "lim:top".to_string(),
            increment: 1,
        },
    );
    assert!(!refused.status.ok);
    assert_eq!(refused.status.code, "out_of_range");
}

#[test]
fn a_value_that_is_not_an_integer_is_refused_and_a_missing_key_counts_as_zero() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // DENOMINATOR: a missing key starts at zero, so a refusal below is about the value.
    assert_eq!(resp(&engine, &["INCRBY", "int:fresh", "7"]), RespValue::Integer(7));
    assert_eq!(resp(&engine, &["DECRBY", "int:fresh", "9"]), RespValue::Integer(-2));

    assert_eq!(
        resp(&engine, &["SET", "int:text", "abc"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        resp(&engine, &["INCRBY", "int:text", "1"]),
        RespValue::Error("ERR value is not an integer".to_string())
    );
    assert_eq!(get(&engine, "int:text"), Some(b"abc".to_vec()));

    let refused = run(
        &engine,
        Command::StringIncrBy {
            key: "int:text".to_string(),
            increment: 1,
        },
    );
    assert!(!refused.status.ok);
    assert_eq!(refused.status.code, "unmatched");
}

#[test]
fn an_increment_and_an_append_keep_the_deadline_the_key_holds() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for (verb, key, arg) in [("INCRBY", "ttl:counter", "1"), ("APPEND", "ttl:text", "tail")] {
        assert_eq!(
            resp(&engine, &["SET", key, "10", "PX", "600000"]),
            RespValue::SimpleString("OK".to_string())
        );
        let before = pttl(&engine, key);
        assert!(before > 0, "{key}: the deadline was never armed ({before})");
        let _ = resp(&engine, &[verb, key, arg]);
        let after = pttl(&engine, key);
        assert!(
            after > 0 && after <= before,
            "{verb} must keep the countdown the key already had, got {after} after {before}"
        );
    }
    // And the new values are what the step computed.
    assert_eq!(get(&engine, "ttl:counter"), Some(b"11".to_vec()));
    assert_eq!(get(&engine, "ttl:text"), Some(b"10tail".to_vec()));
}

#[test]
fn a_key_past_its_deadline_starts_again_from_nothing() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for (verb, key, arg, want) in [
        ("INCRBY", "lapsed:counter", "1", RespValue::Integer(1)),
        ("APPEND", "lapsed:text", "z", RespValue::Integer(1)),
    ] {
        assert_eq!(
            resp(&engine, &["SET", key, "notanumber"]),
            RespValue::SimpleString("OK".to_string())
        );
        engine.shards.with_state_mut(1, |state| {
            let shard = state.expect("shard 1 is loaded");
            let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
            crate::engine::set_expiry(shard, key.to_string(), past);
        });
        assert_eq!(
            resp(&engine, &[verb, key, arg]),
            want,
            "{verb} on a lapsed key must start from nothing, not be refused over bytes no read can see"
        );
    }
}

#[test]
fn an_append_creates_the_key_and_answers_the_new_length() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let created = run(
        &engine,
        Command::StringAppend {
            key: "app:new".to_string(),
            value: b"hello".to_vec(),
        },
    );
    assert!(created.status.ok);
    assert_eq!(created.response, CommandResponse::Integer { value: 5 });
    let grown = run(
        &engine,
        Command::StringAppend {
            key: "app:new".to_string(),
            value: b", world".to_vec(),
        },
    );
    assert_eq!(grown.response, CommandResponse::Integer { value: 12 });
    assert_eq!(get(&engine, "app:new"), Some(b"hello, world".to_vec()));
}

#[test]
fn the_stepped_value_survives_a_restart_with_its_deadline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let open = || {
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        let response = engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1,
            table_name: "string-step".to_string(),
            shard_uri: "local://string-step/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket: 1023,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        });
        assert!(response.status.ok, "load failed: {:?}", response.status);
        engine
    };
    {
        let engine = open();
        assert_eq!(
            resp(&engine, &["SET", "dur:counter", "40", "PX", "600000"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(resp(&engine, &["INCRBY", "dur:counter", "2"]), RespValue::Integer(42));
        assert_eq!(resp(&engine, &["APPEND", "dur:text", "ab"]), RespValue::Integer(2));
        assert_eq!(resp(&engine, &["APPEND", "dur:text", "cd"]), RespValue::Integer(4));
    }
    let engine = open();
    assert_eq!(get(&engine, "dur:counter"), Some(b"42".to_vec()));
    assert_eq!(get(&engine, "dur:text"), Some(b"abcd".to_vec()));
    let left = pttl(&engine, "dur:counter");
    assert!(
        left > 0 && left <= 600_000,
        "the deadline must survive the restart, got {left}"
    );
}
