// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn frame_reuse_retains_useful_capacity_but_releases_excess_compression_space() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let mut inner = store.inner.lock().unwrap();
    for (capacity, length, retain) in [(65_536, 128, true), (4 * 1024 * 1024, 128, false),
        (3 * 1024 * 1024, 2 * 1024 * 1024, true), (4 * 1024 * 1024, 0, false)] {
        let mut bytes = Vec::with_capacity(capacity);
        bytes.resize(length, 7);
        let pointer = bytes.as_ptr();
        recycle_encode_scratch(&mut inner, bytes);
        if retain {
            assert_eq!(inner.encode_scratch.as_ptr(), pointer);
            assert_eq!(inner.encode_scratch.len(), length);
        } else {
            assert_eq!(inner.encode_scratch.capacity(), 0);
        }
    }
}

#[test]
fn compressed_append_releases_a_large_reservation_and_reopens_with_its_payload() {
    let previous_frame = std::env::var_os("TS_WAL_BINARY_FRAME");
    let previous_compression = std::env::var_os("TS_WAL_COMPRESS_RECORDS");
    std::env::set_var("TS_WAL_BINARY_FRAME", "1");
    std::env::set_var("TS_WAL_COMPRESS_RECORDS", "1");
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let payload = vec![13; 2 * 1024 * 1024];
    let (record, log_id) = store.append_with_sync_staged(1,
        Command::StringSet { key: "large-frame".into(), value: Vec::new() }, true,
        vec![StagedBlock { object_id: 42, element_name: None, bytes: payload.clone() }],
    ).unwrap();
    assert_eq!(store.inner.lock().unwrap().encode_scratch.capacity(), 0);
    drop(store);
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    let frame = reopened.read_frame_at_log_id(1, log_id).unwrap().unwrap();
    let recovered = decode_wal_line(&frame).unwrap();
    assert_eq!(recovered, record);
    assert_eq!(recovered.staged_blocks[0].bytes, payload);
    for (key, previous) in [("TS_WAL_BINARY_FRAME", previous_frame),
        ("TS_WAL_COMPRESS_RECORDS", previous_compression)] {
        match previous {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}
