// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE COMMAND THAT PUSHES SEVERAL ELEMENTS ONTO A LIST, OR POPS SEVERAL OFF IT.
//!
//! `ListPushMany` and `ListPopMany` take the place of a `ListPush` or a `ListPop` for every
//! element. What they must keep is what the one-at-a-time path did: the order the elements land in
//! (a left push leaves the last element named at the head), the length a push answers, the elements
//! a pop answers in the order they came off, a pop of more than the list holds answering what it
//! held, and the blocks that are written being the same blocks. This file drives both paths over the
//! same elements and compares what each left, live and after a restart, then pins the encodings and
//! the bounds.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::redis::{execute_redis_command, RespValue};
use crate::types::MAX_LIST_ELEMENTS_PER_COMMAND;
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

fn element(n: usize) -> Vec<u8> {
    format!("element-{n:05}").into_bytes()
}

fn names(items: &[&[u8]]) -> Vec<Vec<u8>> {
    items.iter().map(|item| item.to_vec()).collect()
}

fn count_of(response: ExecuteResponse) -> i64 {
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::Integer { value } => value,
        other => panic!("not a count: {other:?}"),
    }
}

fn members_of(response: ExecuteResponse) -> Vec<Vec<u8>> {
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::Members { members } => members,
        other => panic!("not members: {other:?}"),
    }
}

fn whole_list(engine: &TemporalEngine, key: &str) -> Vec<Vec<u8>> {
    members_of(exec(engine, Command::ListRange { key: key.to_string(), start: 0, stop: -1 }))
}

/// What the one-at-a-time path does for a push: one command per element. Answers the last length.
fn push_one_at_a_time(engine: &TemporalEngine, key: &str, members: &[Vec<u8>], left: bool) -> i64 {
    let mut length = 0;
    for m in members {
        length = count_of(exec(engine, Command::ListPush { key: key.to_string(), member: m.clone(), left }));
    }
    length
}

/// One command per element, stopping at the first empty answer. Answers the elements popped.
fn pop_one_at_a_time(engine: &TemporalEngine, key: &str, count: usize, left: bool) -> Vec<Vec<u8>> {
    let mut popped = Vec::new();
    for _ in 0..count {
        let response = exec(engine, Command::ListPop { key: key.to_string(), left });
        assert!(response.status.ok, "{:?}", response.status);
        match response.response {
            CommandResponse::Bytes { value: Some(value) } => popped.push(value),
            _ => break,
        }
    }
    popped
}

#[test]
fn many_elements_leave_what_the_one_at_a_time_path_leaves_and_answer_the_same() {
    let dir_one = tempfile::tempdir().expect("tempdir");
    let dir_many = tempfile::tempdir().expect("tempdir");
    let one = engine_on(dir_one.path());
    let many = engine_on(dir_many.path());
    let key = "l";

    // Right pushes, then left pushes: the last element named in a left push ends at the head.
    let right = names(&[b"a", b"b", b"c"]);
    let left = names(&[b"x", b"y", b"x"]);
    let length_one = push_one_at_a_time(&one, key, &right, false);
    let length_many = count_of(exec(&many, Command::ListPushMany { key: key.into(), members: right.clone(), left: false }));
    assert_eq!((length_one, length_many), (3, 3));
    let length_one = push_one_at_a_time(&one, key, &left, true);
    let length_many = count_of(exec(&many, Command::ListPushMany { key: key.into(), members: left.clone(), left: true }));
    assert_eq!((length_one, length_many), (6, 6));
    let expected = names(&[b"x", b"y", b"x", b"a", b"b", b"c"]);
    assert_eq!(whole_list(&one, key), expected);
    assert_eq!(whole_list(&many, key), expected);

    // Pops: from the head, then more from the tail than the list holds.
    let head_one = pop_one_at_a_time(&one, key, 2, true);
    let head_many = members_of(exec(&many, Command::ListPopMany { key: key.into(), left: true, count: 2 }));
    assert_eq!(head_one, names(&[b"x", b"y"]));
    assert_eq!(head_many, head_one);
    let tail_one = pop_one_at_a_time(&one, key, 9, false);
    let tail_many = members_of(exec(&many, Command::ListPopMany { key: key.into(), left: false, count: 9 }));
    assert_eq!(tail_one, names(&[b"c", b"b", b"a", b"x"]));
    assert_eq!(tail_many, tail_one, "a pop of more than the list holds answers what it held");
    // The list is empty and so is gone, both ways.
    assert_eq!(count_of(exec(&one, Command::CommonExists { key: key.into() })), 0);
    assert_eq!(count_of(exec(&many, Command::CommonExists { key: key.into() })), 0);
    assert!(members_of(exec(&many, Command::ListPopMany { key: key.into(), left: true, count: 3 })).is_empty());

    // Push again so a restart has something to replay.
    let more = names(&[b"p", b"q"]);
    push_one_at_a_time(&one, key, &more, true);
    exec(&many, Command::ListPushMany { key: key.into(), members: more.clone(), left: true });
    let expected = names(&[b"q", b"p"]);
    assert_eq!(whole_list(&one, key), expected);
    assert_eq!(whole_list(&many, key), expected);

    // The same BLOCKS: the block store took the same number of writes and the same bytes through
    // either path.
    let (blocks_one, blocks_many) = (one.block_store().stats(), many.block_store().stats());
    assert_eq!(
        (blocks_one.writes, blocks_one.bytes_written),
        (blocks_many.writes, blocks_many.bytes_written),
        "the blocks written differ between the one-at-a-time path and the many-element commands"
    );
    assert!(blocks_many.writes >= 8, "the block store took only {} writes", blocks_many.writes);

    // Both survive a restart, which replays the log of each.
    drop(one);
    drop(many);
    let one = engine_on(dir_one.path());
    let many = engine_on(dir_many.path());
    assert_eq!(whole_list(&one, key), expected);
    assert_eq!(whole_list(&many, key), expected);
}

#[test]
fn the_bounds_are_accepted_at_the_limit_and_refused_one_past_it_and_at_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let elements = |n: usize| (0..n).map(element).collect::<Vec<_>>();

    let at = exec(&engine, Command::ListPushMany { key: "k".into(), members: elements(MAX_LIST_ELEMENTS_PER_COMMAND), left: false });
    assert_eq!(count_of(at), MAX_LIST_ELEMENTS_PER_COMMAND as i64);
    let over = exec(&engine, Command::ListPushMany { key: "k2".into(), members: elements(MAX_LIST_ELEMENTS_PER_COMMAND + 1), left: false });
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    // Refused whole: nothing was written.
    assert_eq!(count_of(exec(&engine, Command::ListLen { key: "k2".into() })), 0);
    let none = exec(&engine, Command::ListPushMany { key: "k".into(), members: Vec::new(), left: true });
    assert_eq!((none.status.ok, none.status.code.as_str()), (false, "invalid_argument"), "{:?}", none.status);
    assert_eq!(count_of(exec(&engine, Command::ListLen { key: "k".into() })), MAX_LIST_ELEMENTS_PER_COMMAND as i64);

    let popped = members_of(exec(&engine, Command::ListPopMany { key: "k".into(), left: true, count: MAX_LIST_ELEMENTS_PER_COMMAND }));
    assert_eq!(popped.len(), MAX_LIST_ELEMENTS_PER_COMMAND);
    assert_eq!(popped[0], element(0));
    let over = exec(&engine, Command::ListPopMany { key: "k".into(), left: true, count: MAX_LIST_ELEMENTS_PER_COMMAND + 1 });
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    assert!(!exec(&engine, Command::ListPopMany { key: "k".into(), left: true, count: 0 }).status.ok);
}

#[test]
fn the_two_commands_are_writes_a_client_can_route_and_the_wal_encodes_as_protobuf() {
    let push = Command::ListPushMany { key: "k".into(), members: names(&[b"m", b"n"]), left: true };
    let pop = Command::ListPopMany { key: "k".into(), left: false, count: 3 };
    for command in [&push, &pop] {
        assert!(crate::engine::is_write_command(command), "{command:?} would never reach the log");
        assert_eq!(crate::client::command_routing_key(command).as_deref(), Some("k"));
        let payload = crate::shared_store::command_protobuf_bytes(command).expect("a protobuf encoding");
        assert_eq!(&crate::shared_store::command_from_protobuf_bytes(&payload).unwrap(), command);
    }

    // GOLDEN BYTES of the protobuf messages: field 80 and field 81 of `Command`. A `left` of false
    // is not written (proto3).
    use crate::sdk::v1;
    let encoded = v1::Command {
        kind: Some(v1::command::Kind::ListPushMany(v1::ListPushMany {
            key: "k".to_string(),
            members: names(&[b"m", b"n"]),
            left: true,
        })),
    }
    .encode_to_vec();
    assert_eq!(encoded, vec![0x82, 0x05, 0x0b, 0x0a, 0x01, 0x6b, 0x12, 0x01, 0x6d, 0x12, 0x01, 0x6e, 0x18, 0x01]);
    assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), push);
    let encoded = v1::Command {
        kind: Some(v1::command::Kind::ListPopMany(v1::ListPopMany { key: "k".to_string(), left: false, count: 3 })),
    }
    .encode_to_vec();
    assert_eq!(encoded, vec![0x8a, 0x05, 0x05, 0x0a, 0x01, 0x6b, 0x18, 0x03]);
    assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), pop);

    // GOLDEN JSON: each element is the base64 string a single member is, and the array shape of an
    // element still reads. The single-element commands spell exactly as they did.
    let value = serde_json::to_value(&push).unwrap();
    assert_eq!(value["members"], serde_json::json!(["bQ==", "bg=="]));
    assert_eq!(value["left"], serde_json::json!(true));
    assert_eq!(serde_json::from_value::<Command>(value.clone()).unwrap(), push);
    let mut array_shape = value;
    array_shape["members"] = serde_json::json!([[109], [110]]);
    assert_eq!(serde_json::from_value::<Command>(array_shape).unwrap(), push);
    let value = serde_json::to_value(&pop).unwrap();
    assert_eq!((value["left"].as_bool(), value["count"].as_u64()), (Some(false), Some(3)));
    let single = Command::ListPush { key: "k".into(), member: b"m".to_vec(), left: true };
    let value = serde_json::to_value(&single).unwrap();
    assert_eq!((value["member"].as_str(), value["left"].as_bool()), (Some("bQ=="), Some(true)));
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
fn lpush_rpush_and_lpop_are_one_command_per_run_and_answer_what_they_always_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // LPUSH of several: the last named ends at the head, and the answer is the length.
    let (answer, issued) = resp(&engine, &[b"LPUSH", b"l", b"a", b"b", b"c"]);
    assert_eq!(answer, RespValue::Integer(3));
    assert_eq!(issued.get("ListPushMany"), Some(&1), "{issued:?}");
    assert_eq!(issued.get("ListPush"), None, "{issued:?}");
    assert_eq!(whole_list(&engine, "l"), names(&[b"c", b"b", b"a"]));
    let (answer, _) = resp(&engine, &[b"RPUSH", b"l", b"x", b"y"]);
    assert_eq!(answer, RespValue::Integer(5));

    // LPOP with and without a count, and a count past the end.
    let (answer, issued) = resp(&engine, &[b"LPOP", b"l"]);
    assert_eq!(answer, RespValue::Bulk(Some(b"c".to_vec())));
    assert_eq!(issued.get("ListPopMany"), Some(&1), "{issued:?}");
    let (answer, _) = resp(&engine, &[b"LPOP", b"l", b"2"]);
    assert_eq!(answer, RespValue::Array(vec![RespValue::Bulk(Some(b"b".to_vec())), RespValue::Bulk(Some(b"a".to_vec()))]));
    let (answer, _) = resp(&engine, &[b"RPOP", b"l", b"9"]);
    assert_eq!(answer, RespValue::Array(vec![RespValue::Bulk(Some(b"y".to_vec())), RespValue::Bulk(Some(b"x".to_vec()))]));
    assert_eq!(resp(&engine, &[b"LPOP", b"l"]).0, RespValue::Bulk(None));
    assert_eq!(resp(&engine, &[b"LPOP", b"l", b"0"]).0, RespValue::Bulk(None));

    // More elements than a command takes is several commands: 2,050 are three each way.
    let elements: Vec<Vec<u8>> = (0..2050).map(element).collect();
    let mut args: Vec<&[u8]> = vec![b"RPUSH", b"big"];
    args.extend(elements.iter().map(|e| e.as_slice()));
    let (answer, issued) = resp(&engine, &args);
    assert_eq!(answer, RespValue::Integer(2050));
    assert_eq!(issued.get("ListPushMany"), Some(&3), "{issued:?}");
    assert_eq!(count_of(exec(&engine, Command::ListLen { key: "big".into() })), 2050);
    let (answer, issued) = resp(&engine, &[b"LPOP", b"big", b"2050"]);
    assert_eq!(issued.get("ListPopMany"), Some(&3), "{issued:?}");
    match answer {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2050);
            assert_eq!(items[0], RespValue::Bulk(Some(element(0))));
            assert_eq!(items[2049], RespValue::Bulk(Some(element(2049))));
        }
        other => panic!("not an array: {other:?}"),
    }
    assert_eq!(count_of(exec(&engine, Command::ListLen { key: "big".into() })), 0);
}
