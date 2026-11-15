// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SET'S LENGTH AND ONE MEMBER'S PRESENCE ARE READ FROM THE MEMBER MAP, NOT FROM A LISTING.
//!
//! `SCARD`, `SISMEMBER` and `SMISMEMBER` were each built out of `SetMembers`: the engine cloned
//! every member into a vector (and cached the vector) so the door could take its length or search
//! it. Cost grew with the set for an answer that is one integer. `SetCard` and `SetIsMember` read
//! the length of, and one key in, the ordered map the members already live in.
//!
//! WHAT DID NOT MOVE: every stored byte (these are reads), the answer, and the listing's own
//! path. A key the map holds no entry for, or a shard whose map is switched off for listings, is
//! answered from the listing exactly as before -- the test below drives that arm too.
//!
//! WHAT EACH TEST ASSERTS
//! - `the_point_commands_answer_what_the_listing_answers`: over binary members (empty, long, a
//!   NUL and 0xFF), after removals, for an absent key and a key past its deadline, the two new
//!   commands agree with the listing. The map counter must also have moved by at least the number
//!   of resident-key calls: an answer taken from the fold falls short of it.
//! - `the_point_commands_answer_the_same_with_the_map_switched_off`: the fallback arm.
//! - `the_resp_door_answers_through_the_point_commands`: SCARD / SISMEMBER / SMISMEMBER over the
//!   RESP door, incl. a member that is a prefix of another.
//!
//! No limit is introduced here, so there is no accept-at-limit / refuse-past-it pair.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::redis::{execute_redis_command, RespValue};

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn card(engine: &TemporalEngine, key: &str) -> i64 {
    match run(engine, Command::SetCard { key: key.to_string() }) {
        CommandResponse::Integer { value } => value,
        other => panic!("SetCard answered {other:?}"),
    }
}

fn is_member(engine: &TemporalEngine, key: &str, member: &[u8]) -> i64 {
    match run(
        engine,
        Command::SetIsMember {
            key: key.to_string(),
            member: member.to_vec(),
        },
    ) {
        CommandResponse::Integer { value } => value,
        other => panic!("SetIsMember answered {other:?}"),
    }
}

fn listing(engine: &TemporalEngine, key: &str) -> Vec<Vec<u8>> {
    match run(engine, Command::SetMembers { key: key.to_string() }) {
        CommandResponse::Members { members } => members,
        other => panic!("SetMembers answered {other:?}"),
    }
}

fn add(engine: &TemporalEngine, key: &str, member: &[u8]) {
    run(
        engine,
        Command::SetAdd {
            key: key.to_string(),
            member: member.to_vec(),
        },
    );
}

fn members() -> Vec<Vec<u8>> {
    vec![
        Vec::new(),
        b"a".to_vec(),
        b"ab".to_vec(),
        b"abc".to_vec(),
        vec![0, 255, 7],
        vec![b'm'; 300],
    ]
}

/// Every member in `all` is asked about, and each answer must equal what the listing says.
fn agree_with_the_listing(engine: &TemporalEngine, key: &str, all: &[Vec<u8>]) -> u64 {
    let listed = listing(engine, key);
    assert_eq!(listed.len() as i64, card(engine, key), "{key}: length");
    let mut asked = 1;
    for member in all {
        let want = i64::from(listed.contains(member));
        assert_eq!(want, is_member(engine, key, member), "{key}: {member:?}");
        asked += 1;
    }
    asked
}

#[test]
fn the_point_commands_answer_what_the_listing_answers() {
    let before = crate::engine::execute_on_shard::set_point_answers_from_map();
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let all = members();
    let mut resident_calls = 0;
    for member in &all {
        add(&engine, "k", member);
        // Twice: a set holds a member once.
        add(&engine, "k", member);
    }
    resident_calls += agree_with_the_listing(&engine, "k", &all);
    assert_eq!(all.len() as i64, card(&engine, "k"));
    resident_calls += 1;
    assert_eq!(1, is_member(&engine, "k", b"ab"), "a prefix of another member is a member");
    resident_calls += 1;
    assert_eq!(0, is_member(&engine, "k", b"abcd"), "the next one up is not");
    resident_calls += 1;
    assert_eq!(0, is_member(&engine, "k", b"b"));
    resident_calls += 1;

    // Removals: the length falls, the member is gone, its neighbours stay.
    run(&engine, Command::SetRemove { key: "k".to_string(), member: b"a".to_vec() });
    run(&engine, Command::SetRemove { key: "k".to_string(), member: Vec::new() });
    resident_calls += agree_with_the_listing(&engine, "k", &all);
    assert_eq!((all.len() - 2) as i64, card(&engine, "k"));
    resident_calls += 1;
    assert_eq!(0, is_member(&engine, "k", b"a"));
    resident_calls += 1;
    assert_eq!(1, is_member(&engine, "k", b"ab"));
    resident_calls += 1;

    // The map must have answered, not the fold: at least one count per resident-key call.
    let moved = crate::engine::execute_on_shard::set_point_answers_from_map() - before;
    assert!(
        moved >= resident_calls,
        "the member map answered {moved} of {resident_calls} point reads"
    );

    // An absent key is empty, and says so through the listing's arm.
    assert_eq!(0, card(&engine, "never"));
    assert_eq!(0, is_member(&engine, "never", b"a"));
    assert_eq!(0, is_member(&engine, "never", b""));

    // A key past its deadline is empty, and the lapse is a mutation like any listing's.
    add(&engine, "lapsed", b"x");
    assert_eq!(1, card(&engine, "lapsed"));
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, "lapsed".to_string(), past);
    }
    assert_eq!(0, is_member(&engine, "lapsed", b"x"));
    assert_eq!(0, card(&engine, "lapsed"));

    // The last member out takes the key with it.
    for member in &all {
        run(&engine, Command::SetRemove { key: "k".to_string(), member: member.clone() });
    }
    assert_eq!(0, card(&engine, "k"));
    assert_eq!(0, is_member(&engine, "k", b"ab"));
}

#[test]
fn the_point_commands_answer_the_same_with_the_map_switched_off() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "points".to_string(),
        shard_uri: "local://points/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
    let mut extend_config = std::collections::BTreeMap::new();
    extend_config.insert("set_listing_from_map".to_string(), "off".to_string());
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    extend_config,
                    ..Default::default()
                },
            })
            .ok
    );
    let all = members();
    for member in &all {
        add(&engine, "k", member);
    }
    agree_with_the_listing(&engine, "k", &all);
    assert_eq!(all.len() as i64, card(&engine, "k"));
    run(&engine, Command::SetRemove { key: "k".to_string(), member: b"abc".to_vec() });
    assert_eq!((all.len() - 1) as i64, card(&engine, "k"));
    assert_eq!(0, is_member(&engine, "k", b"abc"));
    assert_eq!(1, is_member(&engine, "k", b"ab"));
}

fn resp(engine: &TemporalEngine, args: &[&[u8]]) -> RespValue {
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.to_vec()).collect();
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

#[test]
fn the_resp_door_answers_through_the_point_commands() {
    let before = crate::engine::execute_on_shard::set_point_answers_from_map();
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    assert_eq!(
        RespValue::Integer(3),
        resp(&engine, &[b"SADD", b"s", b"ab", b"abc", b"x"])
    );
    assert_eq!(RespValue::Integer(3), resp(&engine, &[b"SCARD", b"s"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &[b"SISMEMBER", b"s", b"ab"]));
    assert_eq!(RespValue::Integer(0), resp(&engine, &[b"SISMEMBER", b"s", b"a"]));
    assert_eq!(
        RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(0),
            RespValue::Integer(1),
            RespValue::Integer(1),
        ]),
        resp(&engine, &[b"SMISMEMBER", b"s", b"x", b"y", b"abc", b"x"])
    );
    assert_eq!(RespValue::Integer(0), resp(&engine, &[b"SCARD", b"missing"]));
    assert_eq!(RespValue::Integer(0), resp(&engine, &[b"SISMEMBER", b"missing", b"x"]));
    // SCARD + 2 SISMEMBER + 4 SMISMEMBER arguments all hit the resident key.
    let moved = crate::engine::execute_on_shard::set_point_answers_from_map() - before;
    assert!(moved >= 7, "the door reached the member map {moved} times, wanted at least 7");
}
