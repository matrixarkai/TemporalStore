// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Sorted-set range/remove/multi-score commands and the connection commands QUIT and CLIENT,
//! driven through the RESP dispatch against a real engine. Each test fails if its command's
//! dispatch arm is removed (the arm's absence answers `ERR unsupported command`).

use super::*;
use crate::engine::TemporalEngine;
use crate::types::ExecuteRequest;

fn run(engine: &TemporalEngine, state: &mut RedisCommandState, args: &[&str]) -> RespValue {
    execute_redis_command_with_state(
        args.iter().map(|a| a.as_bytes().to_vec()).collect(),
        1,
        state,
        |command| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            if response.status.ok {
                Ok(response.response)
            } else {
                Err(response.status.message)
            }
        },
    )
}

fn engine_with_zset() -> (TemporalEngine, RedisCommandState) {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut state = RedisCommandState::default();
    // a=1 b=2 c=3 d=4 e=5
    for (score, member) in [("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")] {
        assert_eq!(
            run(&engine, &mut state, &["ZADD", "z", score, member]),
            RespValue::Integer(1)
        );
    }
    (engine, state)
}

fn bulks(items: &[&str]) -> RespValue {
    RespValue::Array(
        items
            .iter()
            .map(|i| RespValue::Bulk(Some(i.as_bytes().to_vec())))
            .collect(),
    )
}

#[test]
fn zrangebyscore_limit_offsets_and_counts() {
    let (e, mut s) = engine_with_zset();
    assert_eq!(
        run(&e, &mut s, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2"]),
        bulks(&["b", "c"])
    );
    // negative count = to the end
    assert_eq!(
        run(&e, &mut s, &["ZRANGEBYSCORE", "z", "(1", "+inf", "LIMIT", "2", "-1"]),
        bulks(&["d", "e"])
    );
    // negative offset = nothing; offset past the end = nothing
    assert_eq!(
        run(&e, &mut s, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "-1", "2"]),
        RespValue::Array(vec![])
    );
    assert_eq!(
        run(&e, &mut s, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "9", "2"]),
        RespValue::Array(vec![])
    );
    // WITHSCORES composes with LIMIT in either order
    assert_eq!(
        run(&e, &mut s, &["ZRANGEBYSCORE", "z", "1", "(3", "LIMIT", "1", "1", "WITHSCORES"]),
        bulks(&["b", "2"])
    );
    assert!(matches!(
        run(&e, &mut s, &["ZRANGEBYSCORE", "z", "0", "1", "LIMIT", "1"]),
        RespValue::Error(_)
    ));
}

#[test]
fn zrevrangebyscore_limit_takes_from_the_high_end() {
    let (e, mut s) = engine_with_zset();
    assert_eq!(
        run(&e, &mut s, &["ZREVRANGEBYSCORE", "z", "+inf", "-inf", "LIMIT", "1", "2"]),
        bulks(&["d", "c"])
    );
    assert_eq!(
        run(&e, &mut s, &["ZREVRANGEBYSCORE", "z", "(5", "2", "WITHSCORES", "LIMIT", "0", "1"]),
        bulks(&["d", "4"])
    );
}

#[test]
fn zremrangebyscore_honours_exclusive_and_infinite_bounds() {
    let (e, mut s) = engine_with_zset();
    // removes b and c only: (1 excludes a, (4 excludes d
    assert_eq!(
        run(&e, &mut s, &["ZREMRANGEBYSCORE", "z", "(1", "(4"]),
        RespValue::Integer(2)
    );
    assert_eq!(run(&e, &mut s, &["ZRANGE", "z", "0", "-1"]), bulks(&["a", "d", "e"]));
    assert_eq!(
        run(&e, &mut s, &["ZREMRANGEBYSCORE", "z", "-inf", "+inf"]),
        RespValue::Integer(3)
    );
    assert_eq!(run(&e, &mut s, &["ZCARD", "z"]), RespValue::Integer(0));
    assert_eq!(
        run(&e, &mut s, &["ZREMRANGEBYSCORE", "missing", "-inf", "+inf"]),
        RespValue::Integer(0)
    );
}

#[test]
fn zremrangebyrank_takes_negative_ranks() {
    let (e, mut s) = engine_with_zset();
    assert_eq!(
        run(&e, &mut s, &["ZREMRANGEBYRANK", "z", "-2", "-1"]),
        RespValue::Integer(2)
    );
    assert_eq!(run(&e, &mut s, &["ZRANGE", "z", "0", "-1"]), bulks(&["a", "b", "c"]));
    assert_eq!(
        run(&e, &mut s, &["ZREMRANGEBYRANK", "z", "0", "0"]),
        RespValue::Integer(1)
    );
    assert_eq!(run(&e, &mut s, &["ZRANGE", "z", "0", "-1"]), bulks(&["b", "c"]));
    assert_eq!(
        run(&e, &mut s, &["ZREMRANGEBYRANK", "z", "5", "9"]),
        RespValue::Integer(0)
    );
}

#[test]
fn zmscore_answers_nil_for_missing_members() {
    let (e, mut s) = engine_with_zset();
    let got = run(&e, &mut s, &["ZMSCORE", "z", "b", "nope", "e"]);
    match got {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::Bulk(Some(b"2".to_vec())));
            assert_eq!(items[1], RespValue::Bulk(None));
            assert_eq!(items[2], RespValue::Bulk(Some(b"5".to_vec())));
        }
        other => panic!("ZMSCORE answered {other:?}"),
    }
    // a missing key is all nil
    assert_eq!(
        run(&e, &mut s, &["ZMSCORE", "missing", "x"]),
        RespValue::Array(vec![RespValue::Bulk(None)])
    );
}

#[test]
fn quit_replies_ok_and_marks_the_connection_closing() {
    let e = TemporalEngine::default();
    e.load_shard(1);
    let mut s = RedisCommandState::default();
    assert!(!s.closing);
    assert_eq!(
        run(&e, &mut s, &["QUIT"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert!(s.closing);
}

#[test]
fn client_setname_getname_setinfo_and_id() {
    let e = TemporalEngine::default();
    e.load_shard(1);
    let mut s = RedisCommandState::default();
    let ok = RespValue::SimpleString("OK".to_string());
    assert_eq!(run(&e, &mut s, &["CLIENT", "GETNAME"]), RespValue::Bulk(None));
    assert_eq!(run(&e, &mut s, &["CLIENT", "SETNAME", "worker-1"]), ok);
    assert_eq!(
        run(&e, &mut s, &["CLIENT", "GETNAME"]),
        RespValue::Bulk(Some(b"worker-1".to_vec()))
    );
    assert!(matches!(
        run(&e, &mut s, &["CLIENT", "SETNAME", "has space"]),
        RespValue::Error(_)
    ));
    assert_eq!(run(&e, &mut s, &["client", "setinfo", "LIB-NAME", "redis-py"]), ok);
    assert_eq!(run(&e, &mut s, &["CLIENT", "SETINFO", "LIB-VER", "5.0"]), ok);
    assert!(matches!(
        run(&e, &mut s, &["CLIENT", "SETINFO", "BOGUS", "x"]),
        RespValue::Error(_)
    ));
    let id = run(&e, &mut s, &["CLIENT", "ID"]);
    assert!(matches!(id, RespValue::Integer(n) if n > 0));
    assert_eq!(run(&e, &mut s, &["CLIENT", "ID"]), id, "id is stable per connection");
    let other = RedisCommandState::default();
    assert_ne!(other.client_id, s.client_id, "ids differ across connections");
    assert!(matches!(
        run(&e, &mut s, &["CLIENT", "NOSUCH"]),
        RespValue::Error(_)
    ));
}
