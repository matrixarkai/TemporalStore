// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WAL FLOOR OF THE SERVED INDEX FILE IS DECODED ONCE PER VERSION OF THE FILE.
//!
//! The retention floor reads the served index file three times in a dumping round, and each read
//! used to decode the whole index and scan every block. The answer is a function of the file's
//! bytes, so it is now kept beside the file's identity. This pins both halves: a second call on
//! the same file is answered without a decode, and the answer always equals the direct
//! computation, including after the file is replaced and after it is removed.
#![allow(clippy::all)]
use super::*;
use crate::engine::storage_lifecycle_methods::{
    served_index_wal_floor, wal_floor_of_index_image, SERVED_INDEX_FLOOR_DECODES,
};

const SHARD: u64 = 1;

fn async_engine(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: SHARD,
                config: crate::control::Config { version: 2, async_storage: true, ..Default::default() },
            })
            .ok
    );
    engine
}

fn write(engine: &TemporalEngine, range: std::ops::Range<usize>) {
    for index in range {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("floor-{index:05}"),
                value: format!("value-{index}").repeat(6).into_bytes(),
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
}

/// Replace the served index file the way the engine does: write a new file, rename it over the old.
fn replace_served_index(engine: &TemporalEngine) -> Vec<u8> {
    let bytes = engine.load_served_index_bytes(SHARD).expect("a loaded shard has a served index");
    let path = engine.index_path(SHARD);
    let temporary = path.with_extension("replacing");
    std::fs::write(&temporary, &bytes).unwrap();
    std::fs::rename(&temporary, &path).unwrap();
    bytes
}

fn direct(bytes: &[u8]) -> Option<u64> {
    wal_floor_of_index_image(&decode_index_bytes(bytes).unwrap())
}

fn decodes() -> u64 {
    SERVED_INDEX_FLOOR_DECODES.with(std::cell::Cell::get)
}

#[test]
fn the_floor_of_the_served_index_is_decoded_once_per_file_version() {
    let dir = tempfile::tempdir().unwrap();
    let engine = async_engine(dir.path());
    write(&engine, 0..120);
    let path = engine.index_path(SHARD);

    let bytes = replace_served_index(&engine);
    let expected = direct(&bytes);
    // CONTROL: the image names log-resident blocks, so the floor is a real number and not the
    // trivial `None` that any wrong answer for an empty image would also give.
    assert!(expected.is_some(), "the image names no log-resident block, so this proves nothing");

    let start = decodes();
    assert_eq!(served_index_wal_floor(&path), expected);
    assert_eq!(decodes() - start, 1, "the first call decodes the file");
    for _ in 0..5 {
        assert_eq!(served_index_wal_floor(&path), expected);
    }
    assert_eq!(decodes() - start, 1, "a call on an unchanged file decoded it again");

    // A new version of the file is a new answer.
    write(&engine, 120..240);
    let bytes = replace_served_index(&engine);
    let expected = direct(&bytes);
    assert_eq!(served_index_wal_floor(&path), expected, "a replaced file read as the old one");
    assert_eq!(decodes() - start, 2, "a replaced file was answered from the memo");
    assert_eq!(served_index_wal_floor(&path), expected);
    assert_eq!(decodes() - start, 2);

    // An absent file names nothing, and does not keep answering from the memo.
    std::fs::remove_file(&path).unwrap();
    assert_eq!(served_index_wal_floor(&path), None, "an absent file named a floor");
}
