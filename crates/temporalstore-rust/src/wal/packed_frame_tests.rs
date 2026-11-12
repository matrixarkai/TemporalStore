// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct SegmentPolicy(Option<u64>);

impl SegmentPolicy {
    fn enter(bytes: u64) -> Self {
        Self(SEGMENT_BYTES_OVERRIDE.with(|value| value.replace(Some(bytes))))
    }
}

impl Drop for SegmentPolicy {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(self.0);
    }
}

fn payload(size: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64 ^ seed;
    (0..size)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            (state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 32) as u8
        })
        .collect()
}

#[test]
fn packed_point_view_seeks_across_footer_slots_without_reading_payload_prefixes() {
    let frame = payload(6 * WAL_BLOCK_BYTES as usize + 51, 7);
    let data = (WAL_BLOCK_BYTES - WAL_BLOCK_FOOTER_BYTES) as usize;
    for offset in [0, 27, data - packed_frames::HEADER_BYTES - 1] {
        let packed = packed_frames::pack(&frame, offset as u64, 0, 11);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("packed.bin");
        let mut stored = vec![0; offset];
        stored.extend_from_slice(&packed);
        fs::write(&path, stored).unwrap();
        let mut file = File::open(path).unwrap();
        let mut reader = packed_frames::PointReader::new(&mut file, offset as u64)
            .unwrap()
            .unwrap();
        let first = data - offset - packed_frames::HEADER_BYTES;
        for position in [
            0,
            first.saturating_sub(5),
            first,
            first + data - 3,
            frame.len() - 7,
        ] {
            let length = 31.min(frame.len() - position);
            reader.seek(SeekFrom::Start(position as u64)).unwrap();
            let mut actual = vec![0; length];
            reader.read_exact(&mut actual).unwrap();
            assert_eq!(actual, frame[position..position + length]);
        }
        assert_eq!(reader.seek(SeekFrom::End(0)).unwrap(), frame.len() as u64);
        assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
        assert!(reader.physical_bytes < 16 * 1024);
    }
}

#[test]
fn packed_point_view_validates_transport_header_and_every_footer() {
    let frame = payload(4 * WAL_BLOCK_BYTES as usize, 9);
    let packed = packed_frames::pack(&frame, 0, 0, 11);
    let data = (WAL_BLOCK_BYTES - WAL_BLOCK_FOOTER_BYTES) as usize;
    for offset in [
        1,
        13,
        data + 7,
        2 * data + WAL_BLOCK_FOOTER_BYTES as usize + 7,
    ] {
        let mut corrupt = packed.clone();
        corrupt[offset] ^= 1;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("packed.bin");
        fs::write(&path, corrupt).unwrap();
        assert!(packed_frames::PointReader::new(&mut File::open(path).unwrap(), 0).is_err());
    }
}

#[test]
fn indexed_carried_reads_select_from_a_large_packed_transport() {
    let _policy = SegmentPolicy::enter(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let staged: Vec<_> = (0..500)
        .map(|index| StagedBlock {
            object_id: index,
            element_name: None,
            bytes: payload(1320, index),
        })
        .collect();
    let (record, log_id) = store
        .append_batch_as_one_record_at(7, Vec::new(), staged.clone(), false)
        .unwrap();
    assert_eq!(
        store.read_at_log_id(7, log_id, 1).unwrap().unwrap(),
        vec![packed_frames::MAGIC]
    );
    for index in [0, 127, 249, 499, 0] {
        let before = store.stats_without_rescan(7).bytes_read;
        let value = store
            .read_carried_block_at_log_id(7, log_id, record.sequence, index, index as u64, None)
            .unwrap()
            .unwrap();
        let CarriedBlockRead::Block(bytes) = value else {
            panic!("expected selected block, not a full record")
        };
        assert_eq!(bytes, staged[index].bytes);
        assert!(store.stats_without_rescan(7).bytes_read - before < 32 * 1024);
    }
}

#[test]
fn half_block_records_pack_without_record_sized_padding() {
    let _policy = SegmentPolicy::enter(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    const RECORDS: usize = 128;
    const VALUE_BYTES: usize = 64 * 1024;
    for index in 0..RECORDS {
        store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: format!("key-{index}"),
                    value: payload(VALUE_BYTES, index as u64),
                },
                false,
            )
            .unwrap();
    }
    store.flush(1).unwrap();
    let path = write_ahead_log_path(dir.path(), 1);
    let (sequence, end) = last_wal_sequence_in(&path).unwrap();
    assert_eq!(sequence, RECORDS as u64);
    assert!(
        end < (RECORDS * VALUE_BYTES * 101 / 100) as u64,
        "block layout must not duplicate a substantial fraction of the payload: {end}"
    );
    let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(records.len(), RECORDS);
    for (index, (log_id, raw)) in records.iter().enumerate() {
        let record = decode_wal_line(raw).unwrap();
        assert_eq!(record.sequence, index as u64 + 1);
        assert_eq!(
            record.command,
            Some(Command::StringSet {
                key: format!("key-{index}"),
                value: payload(VALUE_BYTES, index as u64),
            })
        );
        assert_eq!(
            store.read_frame_at_log_id(1, *log_id).unwrap().as_ref(),
            Some(raw)
        );
    }
    assert_eq!(last_wal_sequence_forward(&path).unwrap(), (sequence, end));
}

#[test]
fn packed_headers_footers_and_payloads_detect_corruption_and_short_reads() {
    let frame = crate::log_framing::encode_frame(&payload(300_000, 9));
    let packed = packed_frames::pack(&frame, 513, 0, 7);
    let read = packed_frames::unpack(&mut std::io::Cursor::new(&packed))
        .unwrap()
        .unwrap();
    assert_eq!(read.bytes, frame);
    assert_eq!(read.physical_bytes, packed.len() as u64);
    let first_footer = (WAL_BLOCK_BYTES - WAL_BLOCK_FOOTER_BYTES) as usize - 513;
    let mut wrong_version =
        decode_block_footer(&packed[first_footer..first_footer + WAL_BLOCK_FOOTER_BYTES as usize])
            .unwrap();
    wrong_version.version = 3;
    assert!(decode_block_footer(&encode_block_footer_message(&wrong_version)).is_none());
    wrong_version.version = 1;
    assert!(decode_block_footer(&encode_block_footer_message(&wrong_version)).is_none());
    let mut missing_footer = packed.clone();
    missing_footer[first_footer..first_footer + WAL_BLOCK_FOOTER_BYTES as usize].fill(0);
    assert!(read_raw_record(&mut std::io::Cursor::new(missing_footer)).is_err());
    let mut reservation = packed[..first_footer].to_vec();
    reservation.resize(first_footer + 1024 * 1024, 0);
    assert!(read_raw_record(&mut std::io::Cursor::new(reservation))
        .unwrap()
        .is_none());
    for at in [1, 12, first_footer + 8, first_footer + 127] {
        let mut bad = packed.clone();
        bad[at] ^= 1;
        assert!(
            read_raw_record(&mut std::io::Cursor::new(bad)).is_err(),
            "byte {at}"
        );
    }
    let mut bad_payload = packed.clone();
    bad_payload[1000] ^= 1;
    let record = packed_frames::unpack(&mut std::io::Cursor::new(bad_payload))
        .unwrap()
        .unwrap();
    assert!(crate::log_framing::decode_line(&record.bytes).is_err());
    for cut in [
        1,
        16,
        first_footer - 1,
        first_footer + 64,
        first_footer + 129,
        packed.len() - 1,
    ] {
        assert!(
            read_raw_record(&mut std::io::Cursor::new(&packed[..cut]))
                .unwrap()
                .is_none(),
            "cut {cut}"
        );
    }
}

#[test]
fn a_multi_block_tail_hint_replays_the_crossing_record_and_ignores_a_torn_tail() {
    let _policy = SegmentPolicy::enter(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    store
        .append_with_sync(
            1,
            Command::StringSet {
                key: "anchor".to_string(),
                value: payload(1024, 1),
            },
            false,
        )
        .unwrap();
    let path = write_ahead_log_path(dir.path(), 1);
    let (_, anchor_end) = last_wal_sequence_in(&path).unwrap();
    let (_, log_id) = store
        .append_with_sync_reporting(
            1,
            Command::StringSet {
                key: "large".to_string(),
                value: payload(2 * 1024 * 1024, 2),
            },
            false,
        )
        .unwrap();
    store.flush(1).unwrap();
    assert_eq!(footer_tail_hint(&path).unwrap(), Some((1, log_id)));
    let complete = last_wal_sequence_in(&path).unwrap();
    assert_eq!(complete.0, 2);
    assert_eq!(complete, last_wal_sequence_forward(&path).unwrap());
    let bytes = std::fs::read(&path).unwrap();
    drop(store);
    let first_footer = (WAL_BLOCK_BYTES - WAL_BLOCK_FOOTER_BYTES) as usize;
    for cut in [
        anchor_end as usize + 3,
        first_footer + 64,
        first_footer + 129,
        complete.1 as usize - 1,
    ] {
        std::fs::write(&path, &bytes[..cut]).unwrap();
        assert_eq!(
            last_wal_sequence_in(&path).unwrap(),
            (1, anchor_end),
            "cut {cut}"
        );
        assert_eq!(
            last_wal_sequence_forward(&path).unwrap(),
            (1, anchor_end),
            "cut {cut}"
        );
    }
}

#[test]
fn prefix_reclaim_preserves_packed_record_ids_and_backward_distances() {
    let _policy = SegmentPolicy::enter(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    for index in 0..48 {
        store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: format!("key-{index}"),
                    value: payload(64 * 1024, index),
                },
                false,
            )
            .unwrap();
    }
    store.flush(1).unwrap();
    let before = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
    let report = store.gc_before_sequence_unchecked(1, 33).unwrap();
    assert!(report.records_removed > 0, "{report:?}");
    let after = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(after, before[32..]);
    for (log_id, raw) in &after {
        assert_eq!(
            store.read_frame_at_log_id(1, *log_id).unwrap().as_ref(),
            Some(raw)
        );
    }
    drop(store);
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    let record = reopened
        .append_with_sync(
            1,
            Command::StringSet {
                key: "after-reclaim".to_string(),
                value: payload(64 * 1024, 49),
            },
            true,
        )
        .unwrap();
    assert_eq!(record.sequence, 49);
    assert_eq!(reopened.scan(1, 0, u64::MAX, u64::MAX).unwrap().len(), 17);
}

#[test]
fn atomic_batches_and_rolled_replay_windows_use_physical_spans() {
    let _policy = SegmentPolicy::enter(DEFAULT_WAL_SEGMENT_BYTES);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    for batch in 0..2 {
        store
            .append_batch_atomic(
                1,
                (0..12)
                    .map(|index| {
                        let index = batch * 12 + index;
                        Command::StringSet {
                            key: format!("key-{index}"),
                            value: payload(64 * 1024, index),
                        }
                    })
                    .collect(),
                true,
            )
            .unwrap();
    }
    assert!(wal_segment_paths(dir.path(), 1).len() >= 3);
    assert_eq!(store.info(1).unwrap().current_sequence, 24);
    let expected = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(expected.len(), 24);
    let mut raw_offset = 0;
    let mut raw_records = Vec::new();
    loop {
        let window = store.scan(1, raw_offset, u64::MAX, 70_000).unwrap();
        if window.is_empty() {
            break;
        }
        let (last_id, raw) = window.last().unwrap();
        raw_offset = last_id + raw.len() as u64;
        raw_records.extend(window);
    }
    assert_eq!(raw_records, expected);
    for (id, raw) in &expected {
        assert_eq!(
            store
                .read_at_log_id(1, *id, raw.len() as u64)
                .unwrap()
                .as_ref(),
            Some(raw)
        );
    }
    let mut position = ReplayPosition::at_start_of_log();
    let mut received = Vec::new();
    loop {
        let (records, truncated, next) = store
            .scan_decoded_window(1, position, 70_000, received.is_empty())
            .unwrap();
        assert!(!records.is_empty());
        received.extend(
            records
                .into_iter()
                .map(|(id, record)| (id, record.sequence)),
        );
        if !truncated {
            break;
        }
        assert!(next.log_id() > position.log_id());
        position = next;
    }
    assert_eq!(
        received,
        expected
            .iter()
            .map(|(id, raw)| (*id, decode_wal_line(raw).unwrap().sequence))
            .collect::<Vec<_>>()
    );
    drop(store);
    std::fs::remove_file(write_ahead_log_path(dir.path(), 1)).unwrap();
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    let info = reopened.info(1).unwrap();
    assert_eq!(info.current_sequence, 24);
    assert!(info.length_bytes > 0);
    assert_eq!(info.persistent_length_bytes, info.length_bytes);
    for (id, raw) in expected {
        assert_eq!(reopened.read_frame_at_log_id(1, id).unwrap(), Some(raw));
    }
    assert_eq!(
        reopened
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "after-missing-active".to_string(),
                    value: vec![1],
                },
                true
            )
            .unwrap()
            .sequence,
        25
    );
}
