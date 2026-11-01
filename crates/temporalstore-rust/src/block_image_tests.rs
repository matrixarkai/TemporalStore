// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `block_image`: the record layout, and the LZ4 block decoder it needs to read stores written with
//! compression on. The LZ4 vectors are hand-built from the format, one per rule.
#![allow(clippy::all)]

use super::*;

fn header() -> BlockImageHeader {
    BlockImageHeader {
        routing_bucket: 744_456_307_475_160_751,
        object_id: 3,
        block_id: 7,
        timestamp_ms: 1_793_556_382_644,
        key: b"workloads_1".to_vec(),
        model_id: 1,
        version: 9,
        wal_sequence: 12,
        ..BlockImageHeader::default()
    }
}

#[test]
fn a_block_round_trips_and_the_sizes_are_set_by_the_encoder() {
    for size in [0_usize, 1, 188, 65_536] {
        let data: Vec<u8> = (0..size).map(|index| (index * 7) as u8).collect();
        // The caller's sizes are wrong on purpose: they must not be trusted.
        let mut given = header();
        given.raw_data_size = 5;
        given.data_size = 5;
        given.compress = BlockCompression::Lz4 as i32;
        let payload = encode_block_image(&given, &data).expect("encode");
        let image = decode_block_image(&payload).expect("decode");
        assert_eq!(image.data, data);
        assert_eq!(image.header.raw_data_size, size as u64);
        assert_eq!(image.header.data_size, size as u64);
        assert_eq!(image.header.compress, BlockCompression::Uncompressed as i32);
        assert_eq!(image.header.routing_bucket, header().routing_bucket);
        assert_eq!(image.header.key, header().key);
        // The stored size the index keeps is the whole payload.
        let header_size = u16::from_le_bytes([payload[0], payload[1]]) as usize;
        assert_eq!(payload.len(), 2 + header_size + size);
    }
}

#[test]
fn a_block_in_the_write_ahead_log_is_refused_in_a_data_stream() {
    let mut in_wal = header();
    in_wal.in_wal = true;
    assert!(encode_block_image(&in_wal, b"x").is_err());
}

#[test]
fn a_record_whose_sizes_disagree_with_its_bytes_is_refused() {
    let payload = encode_block_image(&header(), b"0123456789").unwrap();
    // One byte too many.
    let mut long = payload.clone();
    long.push(0);
    assert!(decode_block_image(&long).is_err());
    // One byte too few.
    assert!(decode_block_image(&payload[..payload.len() - 1]).is_err());
    // Shorter than its own prefix and than its own header.
    assert!(decode_block_image(&[]).is_err());
    assert!(decode_block_image(&[200, 0, 1, 2]).is_err());
    // The control: the untouched payload reads.
    assert!(decode_block_image(&payload).is_ok());
}

#[test]
fn an_address_is_the_slab_above_the_offset() {
    assert_eq!(block_address(7, 4096), (7_u64 << 32) | 4096);
    assert_eq!(block_address(u32::MAX, u32::MAX), u64::MAX);
}

// ---- LZ4: one vector per rule ---------------------------------------------------------------

#[test]
fn lz4_literals_only() {
    let src = [&[0x50_u8][..], b"hello"].concat();
    assert_eq!(lz4_block_decompress(&src, 5).unwrap(), b"hello");
}

#[test]
fn lz4_a_match_copies_what_was_already_out() {
    // "abc" then 9 bytes copied from 3 back.
    let src = [0x35, b'a', b'b', b'c', 3, 0];
    assert_eq!(lz4_block_decompress(&src, 12).unwrap(), b"abcabcabcabc");
}

#[test]
fn lz4_a_match_may_overlap_itself() {
    // 'a' then 7 bytes copied from 1 back (so each reads the byte just written), then 'z'.
    let src = [0x13, b'a', 1, 0, 0x10, b'z'];
    assert_eq!(lz4_block_decompress(&src, 9).unwrap(), b"aaaaaaaaz");
}

#[test]
fn lz4_a_literal_length_of_fifteen_or_more_is_extended() {
    let mut src = vec![0xF0, 5]; // 15 + 5 = 20 literals
    src.extend((0..20_u8).map(|byte| byte + 100));
    let expected: Vec<u8> = (0..20_u8).map(|byte| byte + 100).collect();
    assert_eq!(lz4_block_decompress(&src, 20).unwrap(), expected);
    // And 255 runs: 15 + 255 + 3 = 273.
    let mut long = vec![0xF0, 255, 3];
    long.extend(std::iter::repeat(9_u8).take(273));
    assert_eq!(lz4_block_decompress(&long, 273).unwrap(), vec![9_u8; 273]);
}

#[test]
fn lz4_a_match_length_of_nineteen_or_more_is_extended() {
    // 'x', then a match of 4 + 15 + 255 + 10 = 284 from 1 back.
    let src = [0x1F, b'x', 1, 0, 255, 10];
    assert_eq!(lz4_block_decompress(&src, 285).unwrap(), vec![b'x'; 285]);
}

#[test]
fn lz4_refuses_what_it_cannot_decode_safely() {
    // A zero offset, and an offset past the start of the output.
    assert!(lz4_block_decompress(&[0x10, b'a', 0, 0], 5).is_err());
    assert!(lz4_block_decompress(&[0x10, b'a', 9, 0], 5).is_err());
    // Literals running past the input, and a length running past it.
    assert!(lz4_block_decompress(&[0x50, b'h'], 5).is_err());
    assert!(lz4_block_decompress(&[0xF0], 15).is_err());
    // An output that does not match the stated size, either way.
    let src = [0x35, b'a', b'b', b'c', 3, 0];
    assert!(lz4_block_decompress(&src, 11).is_err());
    assert!(lz4_block_decompress(&src, 13).is_err());
    // The control: the same vector with the right size decodes.
    assert!(lz4_block_decompress(&src, 12).is_ok());
}

#[test]
fn a_block_stored_with_lz4_is_read_back_to_the_raw_bytes() {
    let raw = b"abcabcabcabc".to_vec();
    let compressed = vec![0x35, b'a', b'b', b'c', 3, 0];
    let mut stored = header();
    stored.raw_data_size = raw.len() as u64;
    stored.data_size = compressed.len() as u64;
    stored.compress = BlockCompression::Lz4 as i32;
    let proto = stored.encode_to_vec();
    let mut payload = (proto.len() as u16).to_le_bytes().to_vec();
    payload.extend_from_slice(&proto);
    payload.extend_from_slice(&compressed);
    let image = decode_block_image(&payload).expect("decode");
    assert_eq!(image.data, raw);
    assert_eq!(image.header.compress, BlockCompression::Lz4 as i32);
}
