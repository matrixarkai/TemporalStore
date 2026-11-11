// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct SegmentPolicy(Option<u64>);

impl Drop for SegmentPolicy {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(self.0);
    }
}

#[test]
fn sealed_records_define_the_tail_with_an_empty_or_missing_active_piece() {
    let _policy = SegmentPolicy(SEGMENT_BYTES_OVERRIDE.with(|value| value.replace(Some(1))));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let mut ids = Vec::new();
    for index in 0..2 {
        let (_, id) = store
            .append_with_sync_reporting(
                1,
                Command::StringSet {
                    key: format!("key-{index}"),
                    value: vec![index as u8; 128],
                },
                false,
            )
            .unwrap();
        ids.push(id);
    }
    assert_eq!(wal_segment_paths(dir.path(), 1).len(), 3);
    assert_eq!(store.info(1).unwrap().current_sequence, 2);
    drop(store);
    std::fs::remove_file(write_ahead_log_path(dir.path(), 1)).unwrap();
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    let info = reopened.info(1).unwrap();
    assert_eq!(info.current_sequence, 2);
    assert!(info.length_bytes > 0);
    assert_eq!(info.length_bytes, info.persistent_length_bytes);
    for (index, id) in ids.into_iter().enumerate() {
        let raw = reopened.read_frame_at_log_id(1, id).unwrap().unwrap();
        assert_eq!(decode_wal_line(&raw).unwrap().sequence, index as u64 + 1);
    }
    assert_eq!(
        reopened
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "after-reopen".to_string(),
                    value: vec![3],
                },
                true
            )
            .unwrap()
            .sequence,
        3
    );
}
