// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE FIGURE THE STAGE READS TRACKS WHAT A DEFAULT-WRITTEN SHARD APPENDS AFTER A DUMP.
//!
//! The byte gate reads `undumped_wal_bytes_above_oldest_dirty`. It is built from counters the
//! append paths keep, and a path that kept only some of them left the figure at zero after a dump
//! while the log grew. This writes through the engine's ordinary write path only, dumps, writes more,
//! and compares the figure with the growth of the log measured from the log itself.
#![allow(clippy::all)]
use super::*;

fn write(engine: &TemporalEngine, from: usize, to: usize) {
    for index in from..to {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("figure-{index:05}"),
                value: vec![b'v'; 96],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
}

#[test]
fn the_figure_after_a_dump_is_the_bytes_appended_since_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    write(&engine, 0, 300);
    engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        ..StorageLifecycleRequest::default()
    });
    let before = engine.write_ahead_log_store().stats(1).persistent_bytes;
    write(&engine, 300, 500);
    let grown = engine.write_ahead_log_store().stats(1).persistent_bytes - before;
    // NON-VACUITY: the log grew, and the shard has dirty buckets the figure is measured from.
    assert!(grown > 10_000, "the second batch grew the log by only {grown} bytes");
    let figure = engine.undumped_wal_bytes_above_oldest_dirty(1);
    assert!(
        figure * 10 >= grown * 9 && figure * 10 <= grown * 11,
        "the figure is {figure} bytes after the log grew {grown} bytes since the dump"
    );
}
