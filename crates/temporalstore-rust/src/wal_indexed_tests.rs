// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::io::Write;

fn record(count: usize, size: usize) -> WriteAheadLogRecord {
    WriteAheadLogRecord {
        shard_id: 7,
        sequence: 11,
        command: None,
        metadata: None,
        outcomes: Vec::new(),
        staged_blocks: (0..count)
            .map(|index| StagedBlock {
                object_id: index as u64,
                element_name: match index % 3 {
                    0 => None,
                    1 => Some(Arc::from("")),
                    _ => Some(Arc::from("field-\u{96ea}")),
                },
                bytes: (0..size)
                    .map(|offset| ((offset * 19 + index * 43) % 251) as u8)
                    .collect(),
            })
            .collect(),
    }
}

fn encode_record(record: &WriteAheadLogRecord) -> Vec<u8> {
    let parts = record_parts(record).unwrap();
    let mut encoded = Vec::new();
    encode(record, &parts, &mut encoded).unwrap();
    crate::log_framing::frame_binary_body_in_place(COMPRESSED_RAW_PAYLOAD_MARKER, &mut encoded);
    encoded
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(bytes).unwrap();
    file
}

fn selected(
    file: &mut File,
    cache: &mut Cache,
    record: &WriteAheadLogRecord,
    index: usize,
) -> io::Result<(Option<Vec<u8>>, u64)> {
    let block = &record.staged_blocks[index];
    let mut count = 0;
    let bytes = read(
        file,
        0,
        cache,
        record.shard_id,
        0,
        record.sequence,
        index,
        block.object_id,
        block.element_name.as_deref(),
        &mut count,
    )?;
    Ok((bytes, count))
}

#[test]
fn indexed_stream_is_byte_identical_to_original_protobuf_for_existing_decoders() {
    for (count, size) in [(8, 0), (8, 1024), (500, 1320), (8, 128 * 1024)] {
        let record = record(count, size);
        let parts = record_parts(&record).unwrap();
        let mut expected = Vec::new();
        parts.put(&record, &mut expected).unwrap();
        let framed = encode_record(&record);
        let raw = crate::log_framing::decode_line(&framed).unwrap();
        assert_eq!(raw[0], COMPRESSED_RAW_PAYLOAD_MARKER);
        // Exactly the decoder used before indexed streams existed: bulk size from first
        // frame, then stream fallback. No indexed decoding helper participates here.
        let compressed = &raw[1..];
        let capacity = zstd::zstd_safe::get_frame_content_size(compressed)
            .unwrap()
            .unwrap() as usize;
        let old = zstd::bulk::decompress(compressed, capacity)
            .unwrap_or_else(|_| zstd::stream::decode_all(compressed).unwrap());
        assert_eq!(old, expected);
        assert_eq!(crate::wal::decode_wal_line(&framed).unwrap(), record);
        assert_eq!(
            crate::wal::decode_carried_blocks(&framed).unwrap(),
            record.staged_blocks
        );
    }
}

#[test]
fn indexed_points_select_exact_blocks_with_bounded_warm_reads() {
    let record = record(500, 1320);
    let encoded = encode_record(&record);
    let file = file(&encoded);
    let mut opened = file.reopen().unwrap();
    let mut cache = Cache::default();
    let mut cold = 0;
    for index in [0, 499, 7, 249, 1, 2, 498, 42] {
        let (bytes, count) = selected(&mut opened, &mut cache, &record, index).unwrap();
        assert_eq!(bytes.unwrap(), record.staged_blocks[index].bytes);
        if cold == 0 {
            cold = count;
        } else {
            assert!(count < 4096, "{count}");
        }
    }
    assert!(cold < encoded.len() as u64 / 2);
    assert_eq!(cache.entries.len(), 1);
    assert!(cache.bytes < 16 * 1024);
    println!(
        "INDEXED_READ frame_bytes={} cold_bytes={cold} metadata_bytes={}",
        encoded.len(),
        cache.bytes
    );
}

#[test]
fn indexed_point_checks_identity_sequence_and_selected_checksum() {
    let record = record(500, 1320);
    let mut encoded = encode_record(&record);
    let original = file(&encoded);
    let mut opened = original.reopen().unwrap();
    let mut cache = Cache::default();
    selected(&mut opened, &mut cache, &record, 0).unwrap();
    let mut count = 0;
    for (shard, sequence, index, object, name) in [
        (8, 11, 0, 0, None),
        (7, 12, 0, 0, None),
        (7, 11, 500, 0, None),
        (7, 11, 1, 0, Some("")),
        (7, 11, 1, 1, Some("wrong")),
    ] {
        assert!(read(
            &mut opened,
            0,
            &mut cache,
            shard,
            0,
            sequence,
            index,
            object,
            name,
            &mut count
        )
        .is_err());
    }
    *encoded.last_mut().unwrap() ^= 1;
    let corrupt = file(&encoded);
    let mut opened = corrupt.reopen().unwrap();
    assert!(selected(&mut opened, &mut cache, &record, 499).is_err());
    assert!(crate::wal::decode_wal_line(&encoded).is_err());
}

#[test]
fn directory_corruption_and_truncation_do_not_return_a_block() {
    let record = record(500, 1320);
    let encoded = encode_record(&record);
    let position = encoded.windows(4).position(|v| v == INDEX_MAGIC).unwrap();
    for offset in [
        position + 6,
        position + 12,
        position + 39,
        encoded.len() - 1,
    ] {
        let mut corrupt = encoded.clone();
        corrupt[offset] ^= 1;
        let file = file(&corrupt);
        assert!(selected(
            &mut file.reopen().unwrap(),
            &mut Cache::default(),
            &record,
            499
        )
        .is_err());
    }
    for length in [1, 3, 7, position + 3, position + 64, encoded.len() - 1] {
        let file = file(&encoded[..length]);
        let result = selected(
            &mut file.reopen().unwrap(),
            &mut Cache::default(),
            &record,
            499,
        );
        assert!(!matches!(result, Ok((Some(_), _))), "length={length}");
    }
}

#[test]
fn older_payloads_and_unknown_skippable_indexes_use_the_legacy_reader() {
    let record = record(8, 1024);
    let encoded = crate::log_framing::encode_frame(&super::super::encode(&record).unwrap());
    let old = file(&encoded);
    assert!(selected(
        &mut old.reopen().unwrap(),
        &mut Cache::default(),
        &record,
        0
    )
    .unwrap()
    .0
    .is_none());
    let parts = record_parts(&record).unwrap();
    let mut stream = Vec::new();
    encode(&record, &parts, &mut stream).unwrap();
    assert_eq!(stream[12], 8); // version field tag
    stream[13] = 2;
    crate::log_framing::frame_binary_body_in_place(COMPRESSED_RAW_PAYLOAD_MARKER, &mut stream);
    let unknown = file(&stream);
    assert!(selected(
        &mut unknown.reopen().unwrap(),
        &mut Cache::default(),
        &record,
        0
    )
    .unwrap()
    .0
    .is_none());
    assert_eq!(crate::wal::decode_wal_line(&stream).unwrap(), record);
}

#[test]
fn indexed_directory_cache_is_bounded_and_scoped_to_each_store() {
    let record = record(500, 1320);
    let encoded = encode_record(&record);
    let file = file(&encoded);
    let mut opened = file.reopen().unwrap();
    let mut cache = Cache::default();
    for id in 0..600 {
        read(&mut opened, 0, &mut cache, 7, id, 11, 0, 0, None, &mut 0).unwrap();
        assert!(
            cache.bytes + cache.entries.capacity() * std::mem::size_of::<Cached>() <= CACHE_BYTES
        );
    }
    assert!(cache.entries.len() < 600);
    let mut other = Cache::default();
    assert!(other.get(7, 599, 0, 0).is_none());
}

#[test]
fn indexed_store_reads_follow_stable_log_ids_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let record = record(500, 1320);
    let store = crate::wal::LocalWriteAheadLogStore::new(dir.path());
    let report = store.append_replayed_record(record.clone()).unwrap();
    let log_id = store.log_id_at(7, report.offset).unwrap();
    for store in [
        store.clone(),
        crate::wal::LocalWriteAheadLogStore::new(dir.path()),
    ] {
        let value = store
            .read_carried_block_at_log_id(7, log_id, 11, 499, 499, Some(""))
            .unwrap()
            .unwrap();
        let crate::wal::CarriedBlockRead::Block(bytes) = value else {
            panic!("expected selected block")
        };
        assert_eq!(bytes, record.staged_blocks[499].bytes);
    }
}

#[test]
fn indexed_store_reads_follow_reclaim_after_a_directory_was_cached() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::wal::LocalWriteAheadLogStore::new(dir.path());
    let first = record(500, 1320);
    store.append_replayed_record(first.clone()).unwrap();
    let mut next = first;
    next.sequence = 12;
    next.staged_blocks[499].bytes[0] = 77;
    let report = store.append_replayed_record(next.clone()).unwrap();
    let log_id = store.log_id_at(7, report.offset).unwrap();
    let before = store.resolve_log_id(7, log_id).unwrap().unwrap();
    let check = || {
        let value = store
            .read_carried_block_at_log_id(7, log_id, 12, 499, 499, Some(""))
            .unwrap()
            .unwrap();
        let crate::wal::CarriedBlockRead::Block(bytes) = value else {
            panic!("expected selected block")
        };
        assert_eq!(bytes, next.staged_blocks[499].bytes);
    };
    check();
    store.gc_before_sequence_unchecked(7, 12).unwrap();
    let after = store.resolve_log_id(7, log_id).unwrap().unwrap();
    assert!(after < before);
    check();
}

#[test]
fn point_location_reads_only_candidate_headers_in_a_segmented_log() {
    struct ResetRoll;
    impl Drop for ResetRoll {
        fn drop(&mut self) {
            crate::wal::set_wal_segment_bytes_for_test(None);
        }
    }
    let _reset = ResetRoll;
    crate::wal::set_wal_segment_bytes_for_test(Some(1));
    let dir = tempfile::tempdir().unwrap();
    let store = crate::wal::LocalWriteAheadLogStore::new(dir.path());
    let mut ids = Vec::new();
    for _ in 0..32 {
        let (_, id) = store
            .append_batch_as_one_record_at(7, Vec::new(), record(8, 1024).staged_blocks, false)
            .unwrap();
        ids.push(id);
    }
    assert!(crate::wal::wal_piece_extents_for_test(dir.path(), 7).len() >= 32);
    for id in [ids[0], ids[16], ids[31]] {
        crate::wal::WAL_SEGMENT_HEADER_READS.with(|counter| counter.set(0));
        assert!(store.resolve_log_id(7, id).unwrap().is_some());
        let headers = crate::wal::WAL_SEGMENT_HEADER_READS.with(|counter| counter.get());
        assert!((1..=2).contains(&headers), "{headers}");
    }
    crate::wal::WAL_SEGMENT_HEADER_READS.with(|counter| counter.set(0));
    assert!(store.resolve_log_id(7, u64::MAX).unwrap().is_none());
    assert!(crate::wal::WAL_SEGMENT_HEADER_READS.with(|counter| counter.get()) <= 2);
}
