// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE COMMAND THAT ADDS OR REMOVES SEVERAL MEMBERS OF A SET.
//!
//! `SetAddMany` and `SetRemoveMany` take the place of a `SetIsMember` followed by a `SetAdd` or a
//! `SetRemove` for every member. What they must keep is what the one-at-a-time path did: the count
//! of members that were absent (or present), a member named twice counting once, a member already
//! there writing nothing, and the blocks that are written being the same blocks. This file drives
//! both paths over the same members and compares what each left, live and after a restart, then
//! pins the encodings and the bounds.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::redis::{execute_redis_command, RespValue};
use crate::types::MAX_SET_MEMBERS_PER_COMMAND;
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

fn count_of(response: ExecuteResponse) -> i64 {
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::Integer { value } => value,
        other => panic!("not a count: {other:?}"),
    }
}

fn members_of(engine: &TemporalEngine, key: &str) -> Vec<Vec<u8>> {
    let response = exec(engine, Command::SetMembers { key: key.to_string() });
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::Members { mut members } => {
            members.sort();
            members
        }
        other => panic!("not members: {other:?}"),
    }
}

/// What the one-at-a-time path does for an add: ask, then add what is absent. Answers the count.
fn add_one_at_a_time(engine: &TemporalEngine, key: &str, members: &[Vec<u8>]) -> i64 {
    let mut added = 0;
    for m in members {
        let present = count_of(exec(engine, Command::SetIsMember { key: key.to_string(), member: m.clone() }));
        if present != 1 {
            assert!(exec(engine, Command::SetAdd { key: key.to_string(), member: m.clone() }).status.ok);
            added += 1;
        }
    }
    added
}

fn remove_one_at_a_time(engine: &TemporalEngine, key: &str, members: &[Vec<u8>]) -> i64 {
    let mut removed = 0;
    for m in members {
        let present = count_of(exec(engine, Command::SetIsMember { key: key.to_string(), member: m.clone() }));
        if present == 1 {
            assert!(exec(engine, Command::SetRemove { key: key.to_string(), member: m.clone() }).status.ok);
            removed += 1;
        }
    }
    removed
}

#[test]
fn many_members_leave_what_the_one_at_a_time_path_leaves_and_count_the_same() {
    let dir_one = tempfile::tempdir().expect("tempdir");
    let dir_many = tempfile::tempdir().expect("tempdir");
    let one = engine_on(dir_one.path());
    let many = engine_on(dir_many.path());
    let key = "s";

    // Round one: new members, one named twice, one named three times.
    let round_one: Vec<Vec<u8>> = vec![member(1), member(2), member(3), member(2), member(4), member(2), member(5)];
    let added_one = add_one_at_a_time(&one, key, &round_one);
    let added_many = count_of(exec(&many, Command::SetAddMany { key: key.to_string(), members: round_one.clone() }));
    assert_eq!((added_one, added_many), (5, 5), "five distinct members were absent");

    // Round two: some present, some new, so a member already there writes nothing.
    let round_two: Vec<Vec<u8>> = vec![member(3), member(6), member(1), member(7), member(6)];
    let added_one = add_one_at_a_time(&one, key, &round_two);
    let added_many = count_of(exec(&many, Command::SetAddMany { key: key.to_string(), members: round_two.clone() }));
    assert_eq!((added_one, added_many), (2, 2));

    // Round three: removals, absent members and one named twice.
    let removals: Vec<Vec<u8>> = vec![member(2), member(9), member(4), member(2), member(7)];
    let removed_one = remove_one_at_a_time(&one, key, &removals);
    let removed_many = count_of(exec(&many, Command::SetRemoveMany { key: key.to_string(), members: removals.clone() }));
    assert_eq!((removed_one, removed_many), (3, 3));

    // The same set, answered the same way.
    let expected: Vec<Vec<u8>> = vec![member(1), member(3), member(5), member(6)];
    assert_eq!(members_of(&one, key), expected);
    assert_eq!(members_of(&many, key), expected);
    assert_eq!(count_of(exec(&one, Command::SetCard { key: key.to_string() })), 4);
    assert_eq!(count_of(exec(&many, Command::SetCard { key: key.to_string() })), 4);

    // The same BLOCKS: the block store took the same number of writes and the same bytes through
    // either path (the write-ahead log and the index log differ, by design, in how a request is
    // spelled).
    let (blocks_one, blocks_many) = (one.block_store().stats(), many.block_store().stats());
    assert_eq!(
        (blocks_one.writes, blocks_one.bytes_written),
        (blocks_many.writes, blocks_many.bytes_written),
        "the blocks written differ between the one-at-a-time path and the many-member command"
    );
    // CONTROL: the comparison saw real writes.
    assert!(blocks_many.writes >= 7, "the block store took only {} writes", blocks_many.writes);

    // Both survive a restart, which replays the log of each: the many-member command is replayed
    // as itself.
    drop(one);
    drop(many);
    let one = engine_on(dir_one.path());
    let many = engine_on(dir_many.path());
    assert_eq!(members_of(&one, key), expected);
    assert_eq!(members_of(&many, key), expected);
}

#[test]
fn removing_the_last_members_removes_the_key_as_a_remove_does_and_an_absent_key_removes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    assert_eq!(count_of(exec(&engine, Command::SetRemoveMany { key: "none".into(), members: vec![member(1)] })), 0);
    assert_eq!(count_of(exec(&engine, Command::SetAddMany { key: "k".into(), members: vec![member(1), member(2)] })), 2);
    assert_eq!(count_of(exec(&engine, Command::SetRemoveMany { key: "k".into(), members: vec![member(2), member(1)] })), 2);
    assert_eq!(count_of(exec(&engine, Command::SetCard { key: "k".into() })), 0);
    // The key is gone, as it is when the last member goes one command at a time.
    assert_eq!(count_of(exec(&engine, Command::CommonExists { key: "k".into() })), 0);
}

#[test]
fn the_member_bound_is_accepted_at_the_limit_and_refused_one_past_it_and_at_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let names = |n: usize| (0..n).map(member).collect::<Vec<_>>();

    let at = exec(&engine, Command::SetAddMany { key: "k".into(), members: names(MAX_SET_MEMBERS_PER_COMMAND) });
    assert_eq!(count_of(at), MAX_SET_MEMBERS_PER_COMMAND as i64);
    assert_eq!(count_of(exec(&engine, Command::SetCard { key: "k".into() })), MAX_SET_MEMBERS_PER_COMMAND as i64);

    let over = exec(&engine, Command::SetAddMany { key: "k2".into(), members: names(MAX_SET_MEMBERS_PER_COMMAND + 1) });
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    // Refused whole: nothing was written.
    assert_eq!(count_of(exec(&engine, Command::SetCard { key: "k2".into() })), 0);
    let none = exec(&engine, Command::SetAddMany { key: "k".into(), members: Vec::new() });
    assert_eq!((none.status.ok, none.status.code.as_str()), (false, "invalid_argument"), "{:?}", none.status);

    let at = exec(&engine, Command::SetRemoveMany { key: "k".into(), members: names(MAX_SET_MEMBERS_PER_COMMAND) });
    assert_eq!(count_of(at), MAX_SET_MEMBERS_PER_COMMAND as i64);
    let over = exec(&engine, Command::SetRemoveMany { key: "k".into(), members: names(MAX_SET_MEMBERS_PER_COMMAND + 1) });
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    assert!(!exec(&engine, Command::SetRemoveMany { key: "k".into(), members: Vec::new() }).status.ok);
}

#[test]
fn the_two_commands_are_writes_a_client_can_route_and_the_wal_encodes_as_protobuf() {
    let add = Command::SetAddMany { key: "k".into(), members: vec![b"m".to_vec(), b"n".to_vec()] };
    let remove = Command::SetRemoveMany { key: "k".into(), members: vec![b"m".to_vec(), b"n".to_vec()] };
    for command in [&add, &remove] {
        assert!(crate::engine::is_write_command(command), "{command:?} would never reach the log");
        assert_eq!(crate::client::command_routing_key(command).as_deref(), Some("k"));
        // The payload the shared log and the raft log carry reads back as the command.
        let payload = crate::shared_store::command_protobuf_bytes(command).expect("a protobuf encoding");
        assert_eq!(&crate::shared_store::command_from_protobuf_bytes(&payload).unwrap(), command);
    }

    // GOLDEN BYTES of the protobuf message: field 76 and field 77 of `Command`, each a message of
    // the key (field 1) and the members (field 2, once for each).
    use crate::sdk::v1;
    let encoded = v1::Command {
        kind: Some(v1::command::Kind::SetAddMany(v1::SetAddMany {
            key: "k".to_string(),
            members: vec![b"m".to_vec(), b"n".to_vec()],
        })),
    }
    .encode_to_vec();
    assert_eq!(encoded, vec![0xe2, 0x04, 0x09, 0x0a, 0x01, 0x6b, 0x12, 0x01, 0x6d, 0x12, 0x01, 0x6e]);
    assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), add);
    let encoded = v1::Command {
        kind: Some(v1::command::Kind::SetRemoveMany(v1::SetRemoveMany {
            key: "k".to_string(),
            members: vec![b"m".to_vec(), b"n".to_vec()],
        })),
    }
    .encode_to_vec();
    assert_eq!(encoded, vec![0xea, 0x04, 0x09, 0x0a, 0x01, 0x6b, 0x12, 0x01, 0x6d, 0x12, 0x01, 0x6e]);
    assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), remove);

    // GOLDEN JSON: each member is the base64 string a single member is, and either shape reads.
    let text = serde_json::to_string(&add).unwrap();
    assert_eq!(text, r#"{"kind":"set_add_many","key":"k","members":["bQ==","bg=="]}"#, "{text}");
    assert_eq!(serde_json::from_str::<Command>(&text).unwrap(), add);
    let array_shape = r#"{"kind":"set_add_many","key":"k","members":[[109],[110]]}"#;
    assert_eq!(serde_json::from_str::<Command>(array_shape).unwrap(), add);
    // The single-member command spells exactly as it did.
    let single = Command::SetAdd { key: "k".into(), member: b"m".to_vec() };
    assert_eq!(serde_json::to_string(&single).unwrap(), r#"{"kind":"set_add","key":"k","member":"bQ=="}"#);
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
fn sadd_and_srem_are_one_command_per_run_of_members_and_answer_what_they_always_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // A member named twice and a member already there: counted as the one-at-a-time door counted.
    let (answer, issued) = resp(&engine, &[b"SADD", b"s", b"a", b"b", b"a", b"c"]);
    assert_eq!(answer, RespValue::Integer(3));
    assert_eq!(issued.get("SetAddMany"), Some(&1), "{issued:?}");
    assert_eq!((issued.get("SetIsMember"), issued.get("SetAdd")), (None, None), "{issued:?}");
    let (answer, _) = resp(&engine, &[b"SADD", b"s", b"c", b"d"]);
    assert_eq!(answer, RespValue::Integer(1));
    assert_eq!(count_of(exec(&engine, Command::SetCard { key: "s".into() })), 4);

    let (answer, issued) = resp(&engine, &[b"SREM", b"s", b"a", b"zz", b"d", b"a"]);
    assert_eq!(answer, RespValue::Integer(2));
    assert_eq!(issued.get("SetRemoveMany"), Some(&1), "{issued:?}");
    assert_eq!(members_of(&engine, "s"), vec![b"b".to_vec(), b"c".to_vec()]);

    // More members than a command takes is several commands: 2,050 members are three.
    let names: Vec<Vec<u8>> = (0..2050).map(member).collect();
    let mut args: Vec<&[u8]> = vec![b"SADD", b"big"];
    args.extend(names.iter().map(|name| name.as_slice()));
    let (answer, issued) = resp(&engine, &args);
    assert_eq!(answer, RespValue::Integer(2050));
    assert_eq!(issued.get("SetAddMany"), Some(&3), "{issued:?}");
    assert_eq!(count_of(exec(&engine, Command::SetCard { key: "big".into() })), 2050);
    let mut args: Vec<&[u8]> = vec![b"SREM", b"big"];
    args.extend(names.iter().map(|name| name.as_slice()));
    let (answer, issued) = resp(&engine, &args);
    assert_eq!(answer, RespValue::Integer(2050));
    assert_eq!(issued.get("SetRemoveMany"), Some(&3), "{issued:?}");
    assert_eq!(count_of(exec(&engine, Command::SetCard { key: "big".into() })), 0);
}
