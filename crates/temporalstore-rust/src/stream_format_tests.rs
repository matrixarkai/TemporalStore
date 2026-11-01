// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `stream_format` against the bytes the design's own binary wrote, and against itself where no
//! such bytes exist.
#![allow(clippy::all)]

use super::*;

/// The index stream a single-node cluster of the design's own binary wrote while idle: 4,974
/// records across two full blocks and a partial third. Captured, compressed, never edited.
fn captured() -> Vec<u8> {
    zstd::decode_all(
        include_bytes!("../tests/fixtures/stream_blob_index.zst").as_slice(),
    )
    .expect("the fixture decompresses")
}

#[test]
fn the_checksum_is_the_raw_castagnoli_the_captured_file_uses() {
    // The two stored checksums of the first record and of the header are what the Python probe
    // matched; this holds the Rust spelling to the same answer on the same bytes.
    let bytes = captured();
    let contents = read_blob(&bytes).expect("the captured stream reads");
    // Every record's checksum was verified on the way in, so the function is right on 4,974
    // payloads. The control: the standard, inverting CRC-32C is NOT this checksum.
    let first = &contents.records[0].payload;
    assert_ne!(crate::checksum::crc32c(first), stream_crc(0, first));
    // And it chains: a checksum over two halves is the checksum over the whole.
    let (left, right) = first.split_at(first.len() / 2);
    assert_eq!(stream_crc(stream_crc(0, left), right), stream_crc(0, first));
}

#[test]
fn the_captured_stream_is_read_exactly() {
    let bytes = captured();
    let contents = read_blob(&bytes).expect("the captured stream reads");
    assert_eq!(contents.header.blob_id, 1);
    assert_eq!(contents.header.blob_name, b"DAT-0000000001");
    assert_eq!(contents.records.len(), 4_974);
    assert_eq!(contents.footers.len(), 2);
    // Both footers are for FULL blocks, and each records a record still straddling the join.
    for footer in &contents.footers {
        assert_eq!(footer.block_end as usize, BLOCK_SIZE - FOOTER_SIZE);
    }
    assert_eq!(contents.footers[0].last_record_left_size, 23);
    assert_eq!(contents.footers[1].last_record_left_size, 34);
    assert_eq!(contents.footers[0].last_record_sequence, 2_385);
    assert_eq!(contents.footers[1].last_record_sequence, 4_766);
    // The first record starts at the start of the stream, and ids only grow.
    assert_eq!(contents.records[0].id, 0);
    assert!(contents.records.windows(2).all(|pair| pair[0].id < pair[1].id));
}

/// THE CROSS-LAYER CHECK: the records their binary wrote are `IndexLog` messages, and the schema
/// module that follows that message reads every one of them.
#[test]
fn the_captured_records_are_read_by_the_schema_module() {
    let bytes = captured();
    let contents = read_blob(&bytes).expect("the captured stream reads");
    for (position, record) in contents.records.iter().enumerate() {
        let message = crate::index_log_wire::ProtoIndexLog::decode(record.payload.as_slice())
            .unwrap_or_else(|error| panic!("record {position}: {error}"));
        assert_eq!(message.sequence, position as u64 + 1, "record {position}: sequence");
        let meta = message.meta_item.as_ref().expect("every captured record is a meta record");
        // Their meta record carries the slab (zone) catalog as a map keyed by zone id.
        assert!(!meta.slabs.is_empty(), "record {position}: no zones in the meta item");
        assert!(meta.version >= 1);
    }
}

#[test]
fn our_writer_reproduces_the_captured_bytes() {
    let bytes = captured();
    let contents = read_blob(&bytes).expect("the captured stream reads");
    // Footers are stamped with the time they were written; replay the captured times in order.
    let mut stamps: Vec<(u64, u64)> = contents
        .footers
        .iter()
        .map(|footer| (footer.timestamp_ms, footer.truncated_offset))
        .collect();
    stamps.reverse();
    let mut writer = BlobWriter::new(
        contents.header.clone(),
        Box::new(move || {
            let (timestamp_ms, truncated_offset) = stamps.pop().expect("a footer stamp");
            FooterStamp { timestamp_ms, truncated_offset }
        }),
    )
    .expect("a writer");
    for record in &contents.records {
        let id = writer.append(&record.payload);
        assert_eq!(id, record.id, "the id of a record is where its header starts");
    }
    let ours = writer.into_bytes();
    assert_eq!(ours.len(), bytes.len(), "the lengths differ");
    // On a mismatch, name the first byte, not a 400 KB diff.
    if let Some(at) = ours.iter().zip(&bytes).position(|(a, b)| a != b) {
        panic!("the files first differ at byte {at}: ours {:#04x}, theirs {:#04x}", ours[at], bytes[at]);
    }
}

fn plain_header() -> BlobHeader {
    BlobHeader {
        blob_id: 7,
        blob_name: b"DAT-0000000007".to_vec(),
        timestamp_ms: 1,
        ..BlobHeader::default()
    }
}

fn writer() -> BlobWriter {
    BlobWriter::new(plain_header(), Box::new(|| FooterStamp { timestamp_ms: 99, truncated_offset: 0 }))
        .unwrap()
}

/// Sizes chosen around every edge the layout has: nothing, the header, the varint step at 128, the
/// room left in a block, a record that exactly fills one, and records spanning several.
fn awkward_sizes() -> Vec<usize> {
    let room = BLOCK_SIZE - FOOTER_SIZE;
    vec![
        0, 1, 2, 127, 128, 129, 16_383, 16_384, room - 9, room - 8, room - 7, room - 6, room - 5,
        room - 1, room, room + 1, BLOCK_SIZE, 2 * room, 3 * room + 17, 300_000,
    ]
}

#[test]
fn awkward_records_round_trip_through_the_layout() {
    let mut writer = writer();
    let mut expected = Vec::new();
    for (position, size) in awkward_sizes().into_iter().enumerate() {
        let payload: Vec<u8> = (0..size).map(|index| (index * 31 + position) as u8).collect();
        let id = writer.append(&payload);
        expected.push((id, payload));
    }
    let contents = read_blob(writer.bytes()).expect("the layout reads back");
    assert_eq!(contents.records.len(), expected.len());
    for (read, (id, payload)) in contents.records.iter().zip(&expected) {
        assert_eq!(read.id, *id);
        assert_eq!(&read.payload, payload);
    }
    // Several sizes forced a join: there must be footers, and each full block's data area is full
    // or ends where a record's header would not fit.
    assert!(!contents.footers.is_empty());
    for footer in &contents.footers {
        assert!(footer.block_end as usize <= BLOCK_SIZE - FOOTER_SIZE);
    }
}

#[test]
fn a_record_that_cannot_start_in_the_room_left_starts_on_the_next_block() {
    let room = BLOCK_SIZE - FOOTER_SIZE;
    let mut writer = writer();
    // Leave fewer than a header's worth of bytes in the block.
    let first = writer.append(&vec![1_u8; room - 9]);
    assert_eq!(first, 0);
    let second = writer.append(&vec![2_u8; 10]);
    assert_eq!(second as usize, BLOCK_SIZE, "the record moves to the next block's start");
    let contents = read_blob(writer.bytes()).unwrap();
    assert_eq!(contents.footers.len(), 1);
    // The padding is not data: the footer says the block's data ended before it.
    assert!((contents.footers[0].block_end as usize) < room);
    assert_eq!(contents.footers[0].last_record_left_size, 0);
}

/// THE CONTROLS: every checksum the layout carries is checked, so each corrupted byte is refused.
/// An untouched copy reads, so a refusal below is the corruption and not the fixture.
#[test]
fn every_checksum_in_the_layout_refuses_a_flipped_byte() {
    let mut writer = writer();
    for position in 0..6 {
        writer.append(&vec![position as u8; 90_000]);
    }
    let clean = writer.bytes().to_vec();
    read_blob(&clean).expect("the untouched blob reads");
    let data_start = writer.data_start();
    let flip = |at: usize| {
        let mut copy = clean.clone();
        copy[at] ^= 0x01;
        read_blob(&copy)
    };
    // The blob header's proto.
    assert!(flip(12).is_err(), "a flipped header byte was accepted");
    // A record's payload, early in the first block.
    assert!(flip(data_start + 100).is_err(), "a flipped payload byte was accepted");
    // The first footer's proto, and its struct.
    assert!(flip(data_start + BLOCK_SIZE - 20).is_err(), "a flipped footer byte was accepted");
    assert!(flip(data_start + BLOCK_SIZE - 3).is_err(), "a flipped footer checksum was accepted");
}

#[test]
fn a_blob_must_start_on_a_block_boundary() {
    let header = BlobHeader { start_offset: 5, ..plain_header() };
    assert!(BlobWriter::new(header, Box::new(|| FooterStamp { timestamp_ms: 0, truncated_offset: 0 }))
        .is_err());
}

/// The oplog stream the same binary wrote under load: two records, each one item carrying a block.
fn captured_oplog() -> Vec<u8> {
    zstd::decode_all(
        include_bytes!("../tests/fixtures/stream_blob_oplog.zst").as_slice(),
    )
    .expect("the fixture decompresses")
}

/// The WAL record type this crate already carries has the log item's field numbers, and nothing had
/// ever read a record the design's binary actually wrote with it. This does.
#[test]
fn the_captured_oplog_is_read_by_the_wal_record_type() {
    use crate::wal_record::WalRecord;
    let bytes = captured_oplog();
    let contents = read_blob(&bytes).expect("the captured oplog reads");
    assert_eq!(contents.records.len(), 2);
    assert!(contents.footers.is_empty(), "571 bytes of data: no block was ever filled");
    for (position, record) in contents.records.iter().enumerate() {
        let wal = WalRecord::decode(record.payload.as_slice()).expect("a WAL record");
        assert_eq!(wal.version, 1);
        assert_eq!(wal.sequence, position as u64 + 1);
        assert_eq!(wal.items.len(), 1);
        let item = &wal.items[0];
        assert!(item.block_log, "the item carries its block image");
        assert_eq!(item.model_id, 1);
        assert_eq!(item.object_key.len(), 58);
        assert_eq!(item.block.len(), 188);
        assert!(item.timestamp_ms > 1_700_000_000_000);
        // Re-encoding is canonical: the bytes their binary wrote come back unchanged.
        assert_eq!(wal.encode_to_vec(), record.payload, "record {position} did not re-encode identically");
    }
}

#[test]
fn our_writer_reproduces_the_captured_oplog_bytes() {
    let bytes = captured_oplog();
    let contents = read_blob(&bytes).expect("the captured oplog reads");
    let mut writer = BlobWriter::new(
        contents.header.clone(),
        Box::new(|| FooterStamp { timestamp_ms: 0, truncated_offset: 0 }),
    )
    .unwrap();
    for record in &contents.records {
        assert_eq!(writer.append(&record.payload), record.id);
    }
    assert_eq!(writer.into_bytes(), bytes);
}
