// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE COMMAND THAT ADDS OR REMOVES SEVERAL MEMBERS OF A SORTED SET.
//!
//! `ZSetAddMany` and `ZSetRemoveMany` take the place of a `ZSetAdd` or a `ZSetRemove` for every
//! member. What they must keep is what the one-at-a-time path did: the count of new (or present)
//! members, a member named twice counting once and holding the later score, a re-score counting
//! nothing, and the blocks that are written being the same blocks. This file drives both paths
//! over the same members and compares what each left, live and after a restart, then pins the
//! encodings and the bounds.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::redis::{execute_redis_command, RespValue};
use crate::types::{ZSetScoredMember, MAX_ZSET_MEMBERS_PER_COMMAND};
use prost::Message as _;
use std::cell::RefCell;
use std::collections::BTreeMap;

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

fn exec(engine: &TemporalEngine, command: Command) -> ExecuteResponse {
    engine.execute(ExecuteRequest { shard_id: 1, command })
}

fn member(n: usize) -> Vec<u8> {
    format!("member-{n:05}").into_bytes()
}

fn scored(score: f64, name: &[u8]) -> ZSetScoredMember {
    ZSetScoredMember { member: name.to_vec(), score }
}

fn count_of(response: ExecuteResponse) -> i64 {
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::Integer { value } => value,
        other => panic!("not a count: {other:?}"),
    }
}

/// The whole set in (score, member) order as the engine answers it: member, score, member, score.
fn range_of(engine: &TemporalEngine, key: &str) -> Vec<Vec<u8>> {
    let response = exec(engine, Command::ZSetRange { key: key.to_string(), start: 0, stop: -1, rev: false });
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::Members { members } => members,
        other => panic!("not members: {other:?}"),
    }
}

fn add_one_at_a_time(engine: &TemporalEngine, key: &str, members: &[ZSetScoredMember]) -> i64 {
    members
        .iter()
        .map(|m| count_of(exec(engine, Command::ZSetAdd { key: key.to_string(), member: m.member.clone(), score: m.score })))
        .sum()
}

fn remove_one_at_a_time(engine: &TemporalEngine, key: &str, members: &[Vec<u8>]) -> i64 {
    members
        .iter()
        .map(|m| count_of(exec(engine, Command::ZSetRemove { key: key.to_string(), member: m.clone() })))
        .sum()
}

#[test]
fn many_members_leave_what_the_one_at_a_time_path_leaves_and_count_the_same() {
    let dir_one = tempfile::tempdir().expect("tempdir");
    let dir_many = tempfile::tempdir().expect("tempdir");
    let one = engine_on(dir_one.path());
    let many = engine_on(dir_many.path());
    let key = "z";

    // Round one: new members, one named twice with a later score.
    let round_one = vec![scored(1.0, b"a"), scored(2.0, b"b"), scored(3.0, b"c"), scored(2.5, b"b")];
    let added_one = add_one_at_a_time(&one, key, &round_one);
    let added_many = count_of(exec(&many, Command::ZSetAddMany { key: key.to_string(), members: round_one.clone() }));
    assert_eq!((added_one, added_many), (3, 3), "three distinct members were new");

    // Round two: a re-score counts nothing and a new member counts one.
    let round_two = vec![scored(9.0, b"a"), scored(4.0, b"d"), scored(4.0, b"c")];
    let added_one = add_one_at_a_time(&one, key, &round_two);
    let added_many = count_of(exec(&many, Command::ZSetAddMany { key: key.to_string(), members: round_two.clone() }));
    assert_eq!((added_one, added_many), (1, 1));

    // Round three: removals, an absent member and one named twice.
    let removals: Vec<Vec<u8>> = vec![b"b".to_vec(), b"zz".to_vec(), b"d".to_vec(), b"b".to_vec()];
    let removed_one = remove_one_at_a_time(&one, key, &removals);
    let removed_many = count_of(exec(&many, Command::ZSetRemoveMany { key: key.to_string(), members: removals.clone() }));
    assert_eq!((removed_one, removed_many), (2, 2));

    // The same set in the same order with the same scores.
    let expected = range_of(&one, key);
    assert_eq!(range_of(&many, key), expected);
    assert_eq!(expected.len(), 4, "c and a remain, each with its score: {expected:?}");
    assert_eq!(count_of(exec(&one, Command::ZSetCard { key: key.to_string() })), 2);
    assert_eq!(count_of(exec(&many, Command::ZSetCard { key: key.to_string() })), 2);

    // The same BLOCKS: the block store took the same number of writes and the same bytes through
    // either path.
    let (blocks_one, blocks_many) = (one.block_store().stats(), many.block_store().stats());
    assert_eq!(
        (blocks_one.writes, blocks_one.bytes_written),
        (blocks_many.writes, blocks_many.bytes_written),
        "the blocks written differ between the one-at-a-time path and the many-member command"
    );
    // CONTROL: the comparison saw real writes.
    assert!(blocks_many.writes >= 6, "the block store took only {} writes", blocks_many.writes);

    // Both survive a restart, which replays the log of each.
    drop(one);
    drop(many);
    let one = engine_on(dir_one.path());
    let many = engine_on(dir_many.path());
    assert_eq!(range_of(&one, key), expected);
    assert_eq!(range_of(&many, key), expected);
}

#[test]
fn removing_the_last_members_removes_the_key_as_a_remove_does_and_an_absent_key_removes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    assert_eq!(count_of(exec(&engine, Command::ZSetRemoveMany { key: "none".into(), members: vec![member(1)] })), 0);
    let both = vec![scored(1.0, b"a"), scored(2.0, b"b")];
    assert_eq!(count_of(exec(&engine, Command::ZSetAddMany { key: "k".into(), members: both })), 2);
    assert_eq!(count_of(exec(&engine, Command::ZSetRemoveMany { key: "k".into(), members: vec![b"b".to_vec(), b"a".to_vec()] })), 2);
    assert_eq!(count_of(exec(&engine, Command::ZSetCard { key: "k".into() })), 0);
    assert_eq!(count_of(exec(&engine, Command::CommonExists { key: "k".into() })), 0);
}

#[test]
fn the_member_bound_is_accepted_at_the_limit_and_refused_one_past_it_and_at_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let adds = |n: usize| (0..n).map(|i| scored(i as f64, &member(i))).collect::<Vec<_>>();
    let names = |n: usize| (0..n).map(member).collect::<Vec<_>>();

    let at = exec(&engine, Command::ZSetAddMany { key: "k".into(), members: adds(MAX_ZSET_MEMBERS_PER_COMMAND) });
    assert_eq!(count_of(at), MAX_ZSET_MEMBERS_PER_COMMAND as i64);
    assert_eq!(count_of(exec(&engine, Command::ZSetCard { key: "k".into() })), MAX_ZSET_MEMBERS_PER_COMMAND as i64);

    let over = exec(&engine, Command::ZSetAddMany { key: "k2".into(), members: adds(MAX_ZSET_MEMBERS_PER_COMMAND + 1) });
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    // Refused whole: nothing was written.
    assert_eq!(count_of(exec(&engine, Command::ZSetCard { key: "k2".into() })), 0);
    let none = exec(&engine, Command::ZSetAddMany { key: "k".into(), members: Vec::new() });
    assert_eq!((none.status.ok, none.status.code.as_str()), (false, "invalid_argument"), "{:?}", none.status);

    let at = exec(&engine, Command::ZSetRemoveMany { key: "k".into(), members: names(MAX_ZSET_MEMBERS_PER_COMMAND) });
    assert_eq!(count_of(at), MAX_ZSET_MEMBERS_PER_COMMAND as i64);
    let over = exec(&engine, Command::ZSetRemoveMany { key: "k".into(), members: names(MAX_ZSET_MEMBERS_PER_COMMAND + 1) });
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    assert!(!exec(&engine, Command::ZSetRemoveMany { key: "k".into(), members: Vec::new() }).status.ok);
}

#[test]
fn the_two_commands_are_writes_a_client_can_route_and_the_wal_encodes_as_protobuf() {
    let add = Command::ZSetAddMany { key: "k".into(), members: vec![scored(1.5, b"m"), scored(0.0, b"n")] };
    let remove = Command::ZSetRemoveMany { key: "k".into(), members: vec![b"m".to_vec(), b"n".to_vec()] };
    for command in [&add, &remove] {
        assert!(crate::engine::is_write_command(command), "{command:?} would never reach the log");
        assert_eq!(crate::client::command_routing_key(command).as_deref(), Some("k"));
        let payload = crate::shared_store::command_protobuf_bytes(command).expect("a protobuf encoding");
        assert_eq!(&crate::shared_store::command_from_protobuf_bytes(&payload).unwrap(), command);
    }

    // GOLDEN BYTES of the protobuf messages: field 78 and field 79 of `Command`. A score of zero is
    // not written (proto3), and 1.5 is the eight bytes of its double after the tag 0x11.
    use crate::sdk::v1;
    let encoded = v1::Command {
        kind: Some(v1::command::Kind::ZsetAddMany(v1::ZSetAddMany {
            key: "k".to_string(),
            members: vec![
                v1::ZSetScoredMember { member: b"m".to_vec(), score: 1.5 },
                v1::ZSetScoredMember { member: b"n".to_vec(), score: 0.0 },
            ],
        })),
    }
    .encode_to_vec();
    assert_eq!(
        encoded,
        vec![
            0xf2, 0x04, 0x16, 0x0a, 0x01, 0x6b, 0x12, 0x0c, 0x0a, 0x01, 0x6d, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8,
            0x3f, 0x12, 0x03, 0x0a, 0x01, 0x6e,
        ]
    );
    assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), add);
    let encoded = v1::Command {
        kind: Some(v1::command::Kind::ZsetRemoveMany(v1::ZSetRemoveMany {
            key: "k".to_string(),
            members: vec![b"m".to_vec(), b"n".to_vec()],
        })),
    }
    .encode_to_vec();
    assert_eq!(encoded, vec![0xfa, 0x04, 0x09, 0x0a, 0x01, 0x6b, 0x12, 0x01, 0x6d, 0x12, 0x01, 0x6e]);
    assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), remove);

    // GOLDEN JSON: each member is the base64 string a single member is, beside its score; the
    // list of names is one base64 string each, and the array shape of a name still reads.
    let value = serde_json::to_value(&add).unwrap();
    assert_eq!(
        value["members"],
        serde_json::json!([{"member": "bQ==", "score": 1.5}, {"member": "bg==", "score": 0.0}])
    );
    assert_eq!(serde_json::from_value::<Command>(value).unwrap(), add);
    let value = serde_json::to_value(&remove).unwrap();
    assert_eq!(value["members"], serde_json::json!(["bQ==", "bg=="]));
    assert_eq!(serde_json::from_value::<Command>(value.clone()).unwrap(), remove);
    let mut array_shape = value;
    array_shape["members"] = serde_json::json!([[109], [110]]);
    assert_eq!(serde_json::from_value::<Command>(array_shape).unwrap(), remove);
    // The single-member commands spell exactly as they did.
    let single = Command::ZSetAdd { key: "k".into(), member: b"m".to_vec(), score: 1.5 };
    let value = serde_json::to_value(&single).unwrap();
    assert_eq!((value["member"].as_str(), value["score"].as_f64()), (Some("bQ=="), Some(1.5)));
}

/// One RESP command against the engine, with the engine commands it issued counted by kind.
fn resp(engine: &TemporalEngine, args: &[&[u8]]) -> (RespValue, BTreeMap<String, usize>) {
    let issued = RefCell::new(BTreeMap::<String, usize>::new());
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.to_vec()).collect();
    let answer = execute_redis_command(argv, 1, |command| {
        let text = format!("{command:?}");
        let kind = text.split(|c: char| c == ' ' || c == '{' || c == '(').next().unwrap_or("").to_string();
        *issued.borrow_mut().entry(kind).or_insert(0) += 1;
        let response = engine.execute(ExecuteRequest { shard_id: 1, command });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message.clone())
        }
    });
    (answer, issued.into_inner())
}

#[test]
fn zadd_and_zrem_are_one_command_per_run_of_members_and_answer_what_they_always_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // A member named twice and a re-score: counted as the one-at-a-time door counted.
    let (answer, issued) = resp(&engine, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"a"]);
    assert_eq!(answer, RespValue::Integer(2));
    assert_eq!(issued.get("ZSetAddMany"), Some(&1), "{issued:?}");
    assert_eq!(issued.get("ZSetAdd"), None, "{issued:?}");
    let (answer, _) = resp(&engine, &[b"ZADD", b"z", b"5", b"b", b"6", b"c"]);
    assert_eq!(answer, RespValue::Integer(1));
    assert_eq!(count_of(exec(&engine, Command::ZSetCard { key: "z".into() })), 3);

    let (answer, issued) = resp(&engine, &[b"ZREM", b"z", b"a", b"nope", b"c", b"a"]);
    assert_eq!(answer, RespValue::Integer(2));
    assert_eq!(issued.get("ZSetRemoveMany"), Some(&1), "{issued:?}");
    assert_eq!(count_of(exec(&engine, Command::ZSetCard { key: "z".into() })), 1);

    // More members than a command takes is several commands: 2,050 members are three.
    let names: Vec<Vec<u8>> = (0..2050).map(member).collect();
    let mut args: Vec<&[u8]> = vec![b"ZADD", b"big"];
    for name in &names {
        args.push(b"7");
        args.push(name.as_slice());
    }
    let (answer, issued) = resp(&engine, &args);
    assert_eq!(answer, RespValue::Integer(2050));
    assert_eq!(issued.get("ZSetAddMany"), Some(&3), "{issued:?}");
    assert_eq!(count_of(exec(&engine, Command::ZSetCard { key: "big".into() })), 2050);
    let mut args: Vec<&[u8]> = vec![b"ZREM", b"big"];
    args.extend(names.iter().map(|name| name.as_slice()));
    let (answer, issued) = resp(&engine, &args);
    assert_eq!(answer, RespValue::Integer(2050));
    assert_eq!(issued.get("ZSetRemoveMany"), Some(&3), "{issued:?}");
    assert_eq!(count_of(exec(&engine, Command::ZSetCard { key: "big".into() })), 0);
}
