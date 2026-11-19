// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A LIST WRITE RECORDS ITS BLOCK ONCE, AND THE LIST READS BACK THE SAME WHETHER IT IS REPLAYED OR
//! RELOADED.
//!
//! `ListPush` and `ListSet` filed the block they had just appended twice: once through the unknown
//! write door and once through the install, which files it again. Each filing stages the block's
//! outcome, so the write-ahead-log record carried the same outcome item twice. The first filing is
//! gone; the install files the block and records its outcome once. Nothing about the format or the
//! reader changes: a record with one outcome item is a record the reader always read.
//!
//! WHAT IS HELD HERE.
//!   * each write leaves exactly one `list` outcome item in its record;
//!   * the same writes read back identically from a replayed log (the engine dropped without an
//!     unload, so recovery replays the log) and from an unload followed by a load.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

fn dirs(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    (root.join("cache"), root.join("blocks"), root.join("indexes"))
}

fn open(root: &std::path::Path) -> TemporalEngine {
    let (cache, blocks, indexes) = dirs(root);
    let engine = TemporalEngine::with_local_dirs(1024 * 1024, cache, blocks, indexes);
    engine.load_shard(1);
    engine
}

fn exec(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn whole(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    match exec(engine, Command::ListRange { key: "l".into(), start: 0, stop: -1 }) {
        CommandResponse::Members { members } => members,
        other => panic!("members expected: {other:?}"),
    }
}

/// The `list` outcome items of the newest record that carries any.
fn newest_list_outcomes(engine: &TemporalEngine) -> usize {
    let records = engine.write_ahead_log_store().scan(1, 0, u64::MAX, u64::MAX).unwrap();
    let newest = records
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .filter(|record| record.outcomes.iter().any(|item| item.kind == "list"))
        .last()
        .expect("a record that carries a list outcome");
    newest.outcomes.iter().filter(|item| item.kind == "list").count()
}

/// Seven writes, and the list they leave.
fn drive(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    let mut model: Vec<Vec<u8>> = Vec::new();
    for (n, left) in [(0usize, false), (1, false), (2, true), (3, false), (4, true)] {
        let value = format!("v-{n}").into_bytes();
        exec(engine, Command::ListPush { key: "l".into(), member: value.clone(), left });
        if left {
            model.insert(0, value);
        } else {
            model.push(value);
        }
    }
    exec(engine, Command::ListSet { key: "l".into(), index: 2, member: b"set-at-2".to_vec() });
    model[2] = b"set-at-2".to_vec();
    exec(engine, Command::ListPop { key: "l".into(), left: true });
    model.remove(0);
    model
}

#[test]
fn each_list_write_leaves_one_list_outcome_in_its_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    exec(&engine, Command::ListPush { key: "l".into(), member: b"a".to_vec(), left: false });
    assert_eq!(newest_list_outcomes(&engine), 1, "a ListPush records its block once");
    exec(&engine, Command::ListPush { key: "l".into(), member: b"b".to_vec(), left: true });
    assert_eq!(newest_list_outcomes(&engine), 1, "a ListPush to the head records its block once");
    exec(&engine, Command::ListSet { key: "l".into(), index: 0, member: b"c".to_vec() });
    assert_eq!(newest_list_outcomes(&engine), 1, "a ListSet records its block once");
}

#[test]
fn the_list_reads_back_the_same_from_a_replayed_log_and_from_an_unload_and_a_load() {
    // A replayed log: the engine is dropped without an unload, so the index is not flushed and the
    // tail of the log is what recovery has.
    let replay_dir = tempfile::tempdir().unwrap();
    let model = {
        let engine = open(replay_dir.path());
        let model = drive(&engine);
        assert_eq!(whole(&engine), model);
        model
    };
    let (cache, blocks, indexes) = dirs(replay_dir.path());
    let recovered = TemporalEngine::with_local_dirs(1024 * 1024, cache, blocks, indexes);
    recovered.load_shard(1);
    assert!(
        recovered.replay_installs_for_test() > 0,
        "recovery installed no outcome, so it did not replay the records this test is about"
    );
    assert_eq!(whole(&recovered), model, "after a replayed log");

    // An unload and a load.
    let reload_dir = tempfile::tempdir().unwrap();
    let engine = open(reload_dir.path());
    let model = drive(&engine);
    engine.unload_shard(1);
    engine.load_shard(1);
    assert_eq!(whole(&engine), model, "after an unload and a load");
    // And a write after either still reads back.
    exec(&engine, Command::ListPush { key: "l".into(), member: b"tail".to_vec(), left: false });
    let mut grown = model.clone();
    grown.push(b"tail".to_vec());
    assert_eq!(whole(&engine), grown);
}

fn newest_list_record_len(engine: &TemporalEngine) -> usize {
    let records = engine.write_ahead_log_store().scan(1, 0, u64::MAX, u64::MAX).unwrap();
    records
        .iter()
        .filter(|(_, line)| {
            crate::wal::decode_wal_line(line)
                .map(|record| record.outcomes.iter().any(|item| item.kind == "list"))
                .unwrap_or(false)
        })
        .last()
        .map(|(_, line)| line.len())
        .expect("a record that carries a list outcome")
}

/// The bytes of the log record one list write leaves, for the numbers in the change's description.
/// Run by name; it prints and asserts nothing.
#[test]
#[ignore]
fn the_log_record_bytes_of_a_list_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    exec(&engine, Command::ListPush { key: "l".into(), member: b"0123456789abcdef".to_vec(), left: false });
    let push = newest_list_record_len(&engine);
    exec(&engine, Command::ListSet { key: "l".into(), index: 0, member: b"fedcba9876543210".to_vec() });
    let set = newest_list_record_len(&engine);
    println!("LIST RECORD BYTES push={push} set={set}");
}
