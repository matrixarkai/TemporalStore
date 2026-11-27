// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SYNCHRONOUS WRITE ACKS ON THE LOG FSYNC, SO A BLOCK-STORE COPY THAT NEVER BECAME DURABLE MUST
//! STILL BE READ FROM THE LOG RECORD.
//!
//! The single barrier acks on the log fsync and defers the block fsync. The index already names the
//! real slab address the block append returned, so a crash between the ack and the next dump can
//! lose the slab bytes while the acknowledged value sits in the log. The registration of that block
//! is what makes the log copy reachable, and what pins the record until a dump has made the slab
//! durable. These two tests pin that behaviour BEFORE anything retires a registration earlier than
//! a dump does: whoever changes when a registration goes has to keep both green.
#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 1;
const KEYS: usize = 200;

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

fn set(engine: &TemporalEngine, index: usize) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: format!("slab-{index:04}"),
            value: format!("value-{index:04}-").repeat(6).into_bytes(),
        },
    });
    assert!(response.status.ok, "write {index} refused: {response:?}");
}

fn get(engine: &TemporalEngine, index: usize) -> Option<Vec<u8>> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: format!("slab-{index:04}") },
        })
        .response
    {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response {other:?}"),
    }
}

fn expected(index: usize) -> Vec<u8> {
    format!("value-{index:04}-").repeat(6).into_bytes()
}

/// Empty every block slab file: the bytes the deferred block fsync had not made durable.
fn lose_every_slab(root: &std::path::Path) -> usize {
    let mut emptied = 0;
    for entry in std::fs::read_dir(root.join("blocks")).unwrap().filter_map(Result::ok) {
        let path = entry.path();
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("");
        if name.starts_with("block_segment_") && name.ends_with(".seg") {
            std::fs::write(&path, b"").unwrap();
            emptied += 1;
        }
    }
    emptied
}

#[test]
fn a_block_whose_slab_bytes_are_gone_is_read_from_its_log_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for index in 0..KEYS {
        set(&engine, index);
    }
    // CONTROL: the registrations exist, so there is a log copy to fall back to.
    assert!(
        engine.registration_count_for_test(SHARD) > 0,
        "no registration was held, so the fallback under test cannot exist"
    );
    assert!(lose_every_slab(dir.path()) > 0, "no slab file was found to empty");
    // The cache must not answer for the slab.
    engine.cache().invalidate_shard(SHARD).unwrap();
    for index in 0..KEYS {
        assert_eq!(
            get(&engine, index),
            Some(expected(index)),
            "key {index}: an acknowledged value was not served from its log record"
        );
    }
}

#[test]
fn a_crash_between_the_ack_and_the_next_dump_serves_every_acknowledged_key() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        for index in 0..KEYS {
            set(&engine, index);
        }
        // The process stops here: no unload, no dump. The slab bytes were never made durable.
        assert!(lose_every_slab(dir.path()) > 0, "no slab file was found to empty");
    }
    let reopened = open(dir.path());
    let mut missing = Vec::new();
    for index in 0..KEYS {
        if get(&reopened, index) != Some(expected(index)) {
            missing.push(index);
        }
    }
    assert!(missing.is_empty(), "{} of {KEYS} acknowledged keys were lost: {missing:?}", missing.len());
}
