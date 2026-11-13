// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct Settings(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl Settings {
    fn binary(compressed: bool) -> Self {
        let mut previous = Vec::new();
        for (key, value) in [("TS_WAL_BINARY_FRAME", "1"),
                             ("TS_WAL_INDEXED_BLOCKS", "0"),
                             ("TS_WAL_COMPRESS_RECORDS", if compressed { "1" } else { "0" })] {
            previous.push((key, std::env::var_os(key)));
            std::env::set_var(key, value);
        }
        Self(previous)
    }
}

impl Drop for Settings {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

fn record(blocks: usize, length: usize, repetitive: bool) -> WriteAheadLogRecord {
    WriteAheadLogRecord {
        shard_id: 1, sequence: 17, command: None, metadata: None, outcomes: Vec::new(),
        staged_blocks: (0..blocks).map(|index| {
            let mut state = (index as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
            let bytes = (0..length).map(|offset| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if repetitive && offset % 64 < 16 { (offset % 16) as u8 } else { state as u8 }
            }).collect();
            StagedBlock { object_id: index as u64 + 1, element_name: None, bytes }
        }).collect(),
    }
}

#[test]
fn binary_frame_buffer_matches_existing_bytes_for_raw_and_compressed_records() {
    for compressed in [false, true] {
        let _settings = Settings::binary(compressed);
        let mut actual = vec![99; 37];
        for (blocks, length, repetitive) in [(0, 0, false), (1, 8, true), (1, 128, true),
            (1, 4096, false), (128, 64, true), (512, 1320, true), (1, 2 * 1024 * 1024, true)] {
            let record = record(blocks, length, repetitive);
            let expected = crate::log_framing::encode_record(&encode(&record).unwrap());
            encode_binary_frame_into(&record, &mut actual).unwrap();
            assert_eq!(actual, expected, "compressed={compressed} blocks={blocks} length={length}");
            assert_eq!(crate::wal::decode_wal_line(&actual).unwrap(), record);
        }
    }
}

#[test]
fn compression_trial_into_existing_buffer_keeps_the_existing_selection_rule() {
    for (length, repetitive) in [(0, false), (127, true), (255, true), (256, true),
        (4096, false), (4096, true), (65_536, true)] {
        let input = record(1, length, repetitive).staged_blocks.remove(0).bytes;
        let expected = compress_payload(&input);
        let mut actual = vec![47; 33];
        assert_eq!(compress_payload_into(&input, &mut actual), expected.is_some());
        if let Some(expected) = expected {
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn binary_frame_buffer_reuses_capacity_and_never_appends_previous_records() {
    let _settings = Settings::binary(true);
    let record = record(512, 1320, true);
    let mut output = Vec::new();
    encode_binary_frame_into(&record, &mut output).unwrap();
    let pointer = output.as_ptr();
    let expected = output.clone();
    for _ in 0..8 {
        encode_binary_frame_into(&record, &mut output).unwrap();
        assert_eq!(output.as_ptr(), pointer);
        assert_eq!(output, expected);
    }
}

#[cfg(feature = "alloc-probe")]
#[test]
fn binary_frame_buffer_avoids_the_compressed_output_allocation() {
    let _settings = Settings::binary(true);
    for blocks in [1, 128, 512] {
        let record = record(blocks, 1320, true);
        let mut old_output = Vec::new();
        let mut new_output = Vec::new();
        let old = |out: &mut Vec<u8>| {
            crate::log_framing::encode_record_into(&encode(&record).unwrap(), out);
        };
        old(&mut old_output);
        encode_binary_frame_into(&record, &mut new_output).unwrap();
        let before = crate::alloc_probe::ClassSpan::open();
        old(&mut old_output);
        let previous = before.close().unwrap().total;
        let before = crate::alloc_probe::ClassSpan::open();
        encode_binary_frame_into(&record, &mut new_output).unwrap();
        let current = before.close().unwrap().total;
        assert_eq!(old_output, new_output);
        assert!(current.allocs < previous.allocs);
        assert!(current.alloc_bytes + record.staged_blocks.iter().map(|b| b.bytes.len() as u64).sum::<u64>()
            <= previous.alloc_bytes);
        println!("FRAME_REUSE_ALLOC blocks={blocks} old_allocs={} new_allocs={} old_bytes={} new_bytes={} frame_bytes={}",
            previous.allocs, current.allocs, previous.alloc_bytes, current.alloc_bytes, new_output.len());
    }
}

#[test]
fn binary_frame_buffer_small_growth_stays_below_the_scratch_retention_limit() {
    let mut buffer = Vec::new();
    reserve_encode_capacity(&mut buffer, 700_000);
    let pointer = buffer.as_ptr();
    for length in [700_001, 700_127, 710_000, 720_896] {
        buffer.clear();
        reserve_encode_capacity(&mut buffer, length);
        buffer.resize(length, 13);
        assert_eq!(buffer.as_ptr(), pointer);
        assert!(buffer.capacity() <= 1024 * 1024);
    }
    buffer.clear();
    reserve_encode_capacity(&mut buffer, 720_897);
    assert_eq!(buffer.capacity(), 12 * 64 * 1024);
}
