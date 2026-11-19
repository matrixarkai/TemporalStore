// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THREE LOG-STREAM FILES A DATA WRITE LEAVES BEHIND, READ AND REPRODUCED FROM CAPTURED FILES --
//! tests only.
//!
//! `stream_format` was checked against an idle index stream and an operation log whose only items
//! carried blocks of an internal key. That left three layers resting on hand-built vectors or on
//! the schema alone: a record that carries a user's key and value, a page record (the block image
//! in a data stream), and an index record that names a page. The captures below close all three.
//! They are files captured after writing a hash, a feature
//! point, five thousand feature points in one request, a hash field too large for one block, a
//! key with a deadline, and a key whose fields spread over several pages. Compressed, never
//! edited; each is one whole stream file, header block included.
//!
//! WHAT EACH TEST CAN ONLY PASS ON THE REAL BYTES.
//!   * the writer reproduces every capture byte for byte, footers included, so the layout holds
//!     on records that span blocks and on blocks that are filled;
//!   * every operation-log payload decodes with this crate's record type and re-encodes to the
//!     same bytes, so no field this crate declares is missing and none is mis-numbered -- the
//!     deadline, the block-carrying flag and the feature key and value among them;
//!   * every index payload does the same through the plain schema, and a page item's address and
//!     size point at a page record whose header agrees with the item;
//!   * every page record decodes with `block_image` and re-encodes to the same bytes.
//!
//! WHAT THE CAPTURES DO NOT SHOW: no feature page was captured (the test binary never dumped one), so a
//! feature point's place in a page body is known from the body layout and the hash captures, not
//! from a feature page. See the comment at the page-body test.

#![cfg(test)]
#![allow(clippy::all)]

use prost::Message;

use crate::block_image::{decode_block_image, encode_block_image, BlockImage};
use crate::index_log_record::IndexLogRecord;
use crate::stream_format::{read_blob, stream_crc, BlobContents, BlobWriter, FooterStamp};
use crate::wal_record::WalRecord;

macro_rules! capture {
    ($name:literal) => {
        zstd::decode_all(
            include_bytes!(concat!("../tests/fixtures/stream_blob_data_", $name, ".zst")).as_slice(),
        )
        .expect("the capture decompresses")
    };
}

/// Every capture, by layer. `(label, file bytes)`.
fn oplogs() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("hash", capture!("hash_oplog")),
        ("multipage", capture!("multipage_oplog")),
        ("feature", capture!("feature_oplog")),
        ("feature_many", capture!("feature_many_oplog")),
        ("ttl", capture!("ttl_oplog")),
        ("objmeta", capture!("objmeta_oplog")),
        ("delete_object", capture!("delete_object_oplog")),
    ]
}

fn indexes() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("hash", capture!("hash_index")),
        ("multipage", capture!("multipage_index")),
        ("feature", capture!("feature_index")),
        ("ttl", capture!("ttl_index")),
        ("objmeta", capture!("objmeta_index")),
    ]
}

fn pages() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("hash", capture!("hash_page")),
        ("multipage", capture!("multipage_page")),
        ("objmeta", capture!("objmeta_page")),
    ]
}

fn everything() -> Vec<(String, Vec<u8>)> {
    let mut all = Vec::new();
    for (layer, set) in [("oplog", oplogs()), ("index", indexes()), ("page", pages())] {
        for (label, bytes) in set {
            all.push((format!("{layer}/{label}"), bytes));
        }
    }
    all
}

fn read(name: &str, bytes: &[u8]) -> BlobContents {
    read_blob(bytes).unwrap_or_else(|error| panic!("{name}: {error}"))
}

#[test]
fn every_data_capture_reads_and_carries_the_raw_checksum_the_standard_one_is_not() {
    let expected: &[(&str, usize, usize)] = &[
        // (name, records, footers)
        ("oplog/hash", 3, 0),
        ("oplog/multipage", 22, 1),
        ("oplog/feature", 1, 0),
        ("oplog/feature_many", 1, 1),
        ("oplog/ttl", 2, 0),
        ("oplog/objmeta", 7, 0),
        ("oplog/delete_object", 2, 0),
        ("index/hash", 4, 0),
        ("index/multipage", 37, 0),
        ("index/feature", 3, 0),
        ("index/ttl", 3, 0),
        ("index/objmeta", 10, 0),
        ("page/hash", 1, 0),
        ("page/multipage", 18, 1),
        ("page/objmeta", 3, 0),
    ];
    let all = everything();
    assert_eq!(all.len(), expected.len(), "every capture is counted");
    for ((name, bytes), (want_name, records, footers)) in all.iter().zip(expected) {
        assert_eq!(name.as_str(), *want_name);
        let contents = read(name, bytes);
        assert_eq!(contents.records.len(), *records, "{name}: records");
        assert_eq!(contents.footers.len(), *footers, "{name}: footers");
        // The control: the inverting standard checksum is NOT what these files carry, on any record.
        for record in &contents.records {
            assert_ne!(
                crate::checksum::crc32c(&record.payload),
                stream_crc(0, &record.payload),
                "{name}: the two checksums agree on a record, so this capture cannot tell them apart"
            );
        }
    }
}

#[test]
fn our_writer_reproduces_every_data_capture_byte_for_byte() {
    for (name, bytes) in everything() {
        let contents = read(&name, &bytes);
        // A footer's time and truncation point are inputs to a writer, so replay the captured ones.
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
            assert_eq!(writer.append(&record.payload), record.id, "{name}: where a record starts");
        }
        let ours = writer.into_bytes();
        assert_eq!(ours.len(), bytes.len(), "{name}: lengths differ");
        if let Some(at) = ours.iter().zip(&bytes).position(|(a, b)| a != b) {
            panic!("{name}: first difference at byte {at}: ours {:#04x}, captured {:#04x}", ours[at], bytes[at]);
        }
    }
}

fn decoded_oplog(name: &str, bytes: &[u8]) -> Vec<WalRecord> {
    let contents = read(name, bytes);
    contents
        .records
        .iter()
        .enumerate()
        .map(|(position, record)| {
            let wal = WalRecord::decode(record.payload.as_slice())
                .unwrap_or_else(|error| panic!("{name} record {position}: {error}"));
            assert_eq!(wal.version, 1, "{name} record {position}: version");
            assert_eq!(wal.sequence, position as u64 + 1, "{name} record {position}: sequence");
            assert_eq!(
                wal.encode_to_vec(),
                record.payload,
                "{name} record {position}: did not re-encode identically"
            );
            wal
        })
        .collect()
}

fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// A length-prefixed string, as a key or a value of text is written.
fn prefixed(bytes: &[u8]) -> Vec<u8> {
    let mut out = varint(bytes.len() as u64);
    out.extend_from_slice(bytes);
    out
}

#[test]
fn every_captured_operation_record_is_a_wal_record_and_re_encodes_identically() {
    let mut items = 0;
    for (name, bytes) in oplogs() {
        for wal in decoded_oplog(name, &bytes) {
            items += wal.items.len();
        }
    }
    assert_eq!(items, 3 + 22 + 1 + 5_020 + 2 + 7 + 2, "every item of every capture was decoded");
}

#[test]
fn the_hash_capture_carries_a_key_a_field_and_a_value_as_length_prefixed_text() {
    let (_, bytes) = &oplogs()[0];
    let records = decoded_oplog("hash", bytes);
    let item = |r: usize| records[r].items.iter().exactly_one_item();
    let first = item(0);
    assert_eq!(first.object_key, b"key1");
    assert_eq!(first.model_id, 3);
    assert_eq!(first.key, prefixed(b"field1"));
    assert_eq!(first.value, prefixed(b"value1"));
    assert!(!first.block_log && !first.meta_log && !first.deleted && !first.object_deleted);
    assert!(first.timestamp_ms > 1_700_000_000_000);
    // Two keys, so two routing buckets; the same key keeps its bucket.
    assert_ne!(item(0).routing_bucket, item(1).routing_bucket);
    assert_eq!(item(0).routing_bucket, item(2).routing_bucket);
    assert_eq!(item(2).key, prefixed(b"field2"));
}

trait ExactlyOneItem<'a> {
    fn exactly_one_item(self) -> &'a crate::wal_record::WalItem;
}
impl<'a, I: Iterator<Item = &'a crate::wal_record::WalItem>> ExactlyOneItem<'a> for I {
    fn exactly_one_item(mut self) -> &'a crate::wal_record::WalItem {
        let item = self.next().expect("one item");
        assert!(self.next().is_none(), "exactly one item");
        item
    }
}

#[test]
fn a_captured_feature_point_is_a_varint_timestamp_key_and_a_length_prefixed_message_value() {
    // One point at 777: the key is the timestamp as a varint, the value is the four-field message
    // behind its own length. This crate's row encoder writes the same message.
    let (_, bytes) = &oplogs()[2];
    let records = decoded_oplog("feature", bytes);
    let item = records[0].items.iter().exactly_one_item();
    assert_eq!(item.object_key, b"9");
    assert_eq!(item.model_id, 2);
    assert_eq!(item.key, varint(777));
    let message = crate::types::SequenceFeatureRow {
        timestamp_ms: 777,
        gid: 1,
        action_type: 2,
        duration: 3,
        author_id: 1,
    }
    .encode_feature_proto_value();
    assert_eq!(item.value, prefixed(&message));

    // ONE record of 5,020 items under one object key: 5,010 points added, then the ten oldest
    // trimmed away -- each trim is the same item with the deleted flag and an empty value.
    let (_, bytes) = &oplogs()[3];
    let records = decoded_oplog("feature_many", bytes);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].items.len(), 5_020);
    // The captured message omits a field whose value is zero (the first point is an EMPTY message),
    // where this crate's row encoder writes every field.
    let row = |i: u64| {
        let mut out = Vec::new();
        if i != 0 {
            for field in [1_u8, 2, 3, 4] {
                out.push(field << 3);
                out.extend_from_slice(&varint(i));
            }
        }
        out
    };
    for (position, item) in records[0].items.iter().enumerate() {
        let position = position as u64;
        assert_eq!(item.object_key, b"uid");
        assert_eq!(item.model_id, 2);
        if position < 5_010 {
            assert!(!item.deleted, "point {position}");
            assert_eq!(item.key, varint(position), "point {position}: key");
            assert_eq!(item.value, prefixed(&row(position)), "point {position}: value");
        } else {
            let trimmed = position - 5_010;
            assert!(item.deleted, "trim {trimmed}");
            assert_eq!(item.key, varint(trimmed), "trim {trimmed}: the oldest goes first");
            assert_eq!(item.value, prefixed(b""), "trim {trimmed}: an empty value");
        }
    }
}

#[test]
fn a_captured_deadline_and_a_captured_block_item_are_the_fields_this_crate_names() {
    let (_, bytes) = &oplogs()[4];
    let records = decoded_oplog("ttl", bytes);
    let write = records[0].items.iter().exactly_one_item();
    assert!(write.block_log, "the first item carries the block");
    assert_eq!(write.object_key, b"ttl_key");
    assert!(write.block.starts_with(&[0x00, 0x09]), "a text block opens with a flag byte and a length");
    assert!(write.block.windows(9).any(|w| w == b"ttl_vaule"), "the value is inside the block");
    assert_eq!(write.ttl, 0, "no deadline on the write itself");
    let expire = records[1].items.iter().exactly_one_item();
    assert!(expire.meta_log, "the deadline travels as a metadata item");
    assert_eq!(expire.ttl, expire.timestamp_ms + 10_000, "a deadline is an absolute millisecond");
    assert!(expire.value.is_empty() && !expire.block_log);

    // A value too large for one block: the item is intact across the join.
    let (_, bytes) = &oplogs()[1];
    let records = decoded_oplog("multipage", bytes);
    let big = records[0].items.iter().exactly_one_item();
    assert_eq!(big.key, prefixed(b"big_field"));
    assert_eq!(big.value.len(), 204_803, "a three-byte length and 204,800 bytes");
    assert!(big.value[3..].iter().all(|byte| *byte == b'a'));
    let contents = read("multipage", bytes);
    assert_eq!(contents.records[0].payload.len() > crate::stream_format::BLOCK_SIZE, true, "it spans blocks");
}

fn decoded_index(name: &str, bytes: &[u8]) -> Vec<IndexLogRecord> {
    let contents = read(name, bytes);
    contents
        .records
        .iter()
        .enumerate()
        .map(|(position, record)| {
            let message = IndexLogRecord::decode(record.payload.as_slice())
                .unwrap_or_else(|error| panic!("{name} record {position}: {error}"));
            assert_eq!(message.version, 1, "{name} record {position}: version");
            assert_eq!(message.sequence, position as u64 + 1, "{name} record {position}: sequence");
            assert_eq!(
                message.encode_to_vec(),
                record.payload,
                "{name} record {position}: did not re-encode identically"
            );
            message
        })
        .collect()
}

#[test]
fn every_captured_index_record_is_a_plain_schema_record_and_re_encodes_identically() {
    let mut page_items = 0;
    let mut meta_records = 0;
    for (name, bytes) in indexes() {
        for record in decoded_index(name, &bytes) {
            page_items += record.items.len();
            meta_records += usize::from(record.meta_item.is_some());
        }
    }
    // hash: one page. multipage: twelve pages, eleven deletions when they were compacted into one,
    // that page's new address, and five more pages -- 29 items. feature and ttl never dumped.
    // objmeta: three pages, one of them re-announced after a dump, and six slab-catalog records.
    assert_eq!(page_items, 1 + 29 + 4, "page items across the captures");
    assert_eq!(meta_records, 3 + 19 + 3 + 3 + 6, "meta records across the captures");
}

/// The page record a page item addresses: the zone is the stream's blob id, the low half is the
/// stream offset the record starts at (footers counted), and the size is the whole record payload.
fn page_record_at<'a>(contents: &'a BlobContents, address: u64) -> &'a crate::stream_format::StreamRecord {
    assert_eq!(address >> 32, contents.header.blob_id, "the zone is the blob id of the page stream");
    contents
        .records
        .iter()
        .find(|record| record.id == (address & 0xFFFF_FFFF))
        .unwrap_or_else(|| panic!("no page record starts at offset {}", address & 0xFFFF_FFFF))
}

#[test]
fn a_page_item_names_the_page_record_it_points_at_and_the_two_agree() {
    for (label, index_bytes, page_bytes) in [
        ("hash", capture!("hash_index"), capture!("hash_page")),
        ("multipage", capture!("multipage_index"), capture!("multipage_page")),
        ("objmeta", capture!("objmeta_index"), capture!("objmeta_page")),
    ] {
        let page_stream = read(label, &page_bytes);
        let mut matched = 0;
        let mut deletions = 0;
        for record in decoded_index(label, &index_bytes) {
            for item in &record.items {
                if item.deleted {
                    // A deletion names no address: the page it retires is named by its ids.
                    assert_eq!(item.address, 0);
                    deletions += 1;
                    continue;
                }
                let found = page_record_at(&page_stream, item.address);
                assert_eq!(found.payload.len() as u32, item.size, "{label}: the size is the whole record");
                let image = decode_block_image(&found.payload).expect("a page record");
                assert_eq!(image.header.routing_bucket, record.routing_bucket, "{label}: the bucket");
                assert_eq!(image.header.block_id, item.block_id, "{label}: the page id");
                assert_eq!(image.header.model_id, item.model_id, "{label}: the model");
                assert_eq!(image.header.wal_sequence, record.wal_sequence, "{label}: the log position it was dumped at");
                matched += 1;
            }
        }
        let (want_matched, want_deleted) = match label {
            "hash" => (1, 0),
            "multipage" => (18, 11),
            _ => (4, 0),
        };
        assert_eq!(matched, want_matched, "{label}: every live page item was followed");
        assert_eq!(deletions, want_deleted, "{label}: deletions");
    }
}

fn images(name: &str, bytes: &[u8]) -> Vec<(Vec<u8>, BlockImage)> {
    read(name, bytes)
        .records
        .into_iter()
        .map(|record| {
            let image = decode_block_image(&record.payload)
                .unwrap_or_else(|error| panic!("{name} at {}: {error}", record.id));
            (record.payload, image)
        })
        .collect()
}

#[test]
fn every_captured_page_record_is_reproduced_byte_for_byte_by_the_block_codec() {
    let mut count = 0;
    for (name, bytes) in pages() {
        for (payload, image) in images(name, &bytes) {
            let again = encode_block_image(&image.header, &image.data).expect("encodes");
            assert_eq!(again, payload, "{name}: a page record did not re-encode identically");
            // Uncompressed.
            assert_eq!(image.header.compress, 0);
            assert_eq!(image.header.raw_data_size, image.data.len() as u64);
            assert_eq!(image.header.object_id, 0);
            assert!(!image.header.in_wal);
            count += 1;
        }
    }
    assert_eq!(count, 1 + 18 + 3);
}

/// One element of a page body: `varint len | key | varint len | value | varint cluster | varint
/// deleted | varint timestamp`. READ FROM THE BODY LAYOUT AND PROVED ON THE HASH AND MULTI-PAGE
/// CAPTURES, whose keys and values are text. A feature page would carry the same layout with the
/// key a varint timestamp; none was captured.
fn body_elements(data: &[u8]) -> Vec<(Vec<u8>, Vec<u8>, u64, u64, u64)> {
    fn read_varint(data: &[u8], at: &mut usize) -> u64 {
        let mut value = 0_u64;
        let mut shift = 0;
        loop {
            let byte = data[*at];
            *at += 1;
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }
    let mut out = Vec::new();
    let mut at = 0;
    while at < data.len() {
        let key_len = read_varint(data, &mut at) as usize;
        let key = data[at..at + key_len].to_vec();
        at += key_len;
        let value_len = read_varint(data, &mut at) as usize;
        let value = data[at..at + value_len].to_vec();
        at += value_len;
        let cluster = read_varint(data, &mut at);
        let deleted = read_varint(data, &mut at);
        let timestamp = read_varint(data, &mut at);
        out.push((key, value, cluster, deleted, timestamp));
    }
    assert_eq!(at, data.len(), "the body is exactly its elements");
    out
}

#[test]
fn a_captured_page_body_holds_the_elements_the_operation_log_recorded_in_order() {
    // hash: one page for `key1` holding both fields, in the order they were written.
    let oplog = decoded_oplog("hash", &oplogs()[0].1);
    let page = images("hash", &pages()[0].1);
    assert_eq!(page.len(), 1);
    let elements = body_elements(&page[0].1.data);
    let wrote: Vec<_> = oplog
        .iter()
        .flat_map(|record| record.items.iter())
        .filter(|item| item.object_key == b"key1")
        .collect();
    assert_eq!(elements.len(), wrote.len());
    for ((key, value, cluster, deleted, timestamp), item) in elements.iter().zip(wrote) {
        assert_eq!((key, value), (&item.key, &item.value));
        assert_eq!((*cluster, *deleted), (0, 0));
        // The page keeps the property's timestamp in nanoseconds; the log keeps it in milliseconds.
        assert_eq!(timestamp / 1_000_000, item.timestamp_ms, "the two clocks are one clock");
    }
    assert_eq!(page[0].1.header.key, b"key1");
    assert_eq!(page[0].1.header.model_id, 3);
    assert_eq!(page[0].1.header.wal_sequence, 3, "dumped after the third record");

    // multipage: page 0 holds the big field, each later page holds one small field.
    let pages_read = images("multipage", &pages()[1].1);
    assert_eq!(pages_read.len(), 18);
    let first = body_elements(&pages_read[0].1.data);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, prefixed(b"big_field"));
    assert_eq!(first[0].1.len(), 204_803);
    for (n, (_, image)) in pages_read.iter().enumerate().skip(1).take(11) {
        let _ = n;
        let elements = body_elements(&image.data);
        assert_eq!(elements.len(), 1);
    }
    // Versions only grow.
    let versions: Vec<u64> = pages_read.iter().map(|(_, image)| image.header.version).collect();
    assert!(versions.windows(2).all(|pair| pair[0] <= pair[1]));
    // Page ids are small and dense, and a compaction rewrites page 1 after page 11, so the order of
    // records is not the order of ids.
    let ids: Vec<u32> = pages_read.iter().map(|(_, image)| image.header.block_id).collect();
    assert_eq!(ids[0], 0);
    assert!(ids.iter().all(|id| *id <= 11));
}

#[test]
fn a_captured_deadline_reaches_the_index_as_an_object_item_and_a_delete_as_a_flagged_item() {
    // The operation log records a deadline as a metadata item; the index records it as an object
    // item at the log position that deadline was written at.
    let oplog = decoded_oplog("objmeta", &oplogs()[5].1);
    let index = decoded_index("objmeta", &indexes()[4].1);
    let mut deadlines: Vec<(u64, u64)> = Vec::new();
    let mut offsets: Vec<u64> = Vec::new();
    for record in &oplog {
        for item in record.items.iter().filter(|item| item.meta_log) {
            deadlines.push((record.sequence, item.ttl));
            // The expiry was asked for in relative milliseconds; the log keeps the absolute one.
            offsets.push(item.ttl - item.timestamp_ms);
        }
    }
    assert_eq!(offsets, vec![100, 200, 10_000]);
    let held: Vec<(u64, u64)> = index
        .iter()
        .flat_map(|record| record.object_items.iter().map(move |object| (record.wal_sequence, object.ttl)))
        .collect();
    assert_eq!(held.len(), 2, "two of the three deadlines had been dumped when the index was read");
    for (wal_sequence, ttl) in &held {
        assert!(
            deadlines.contains(&(*wal_sequence, *ttl)),
            "the object item at log position {wal_sequence} carries a deadline the log never wrote: {ttl}"
        );
    }
    // The object item carries nothing but the deadline: no version, no id.
    for record in &index {
        for object in &record.object_items {
            assert_eq!((object.version, object.object_id), (0, 0));
        }
    }

    // A whole-object delete is one item with the flag, an empty key and an empty value.
    let records = decoded_oplog("delete_object", &oplogs()[6].1);
    assert_eq!(records.len(), 2);
    let write = records[0].items.iter().exactly_one_item();
    let delete = records[1].items.iter().exactly_one_item();
    assert!(delete.object_deleted && !delete.deleted);
    assert_eq!((delete.object_key.as_slice(), delete.model_id), (b"key1".as_slice(), 3));
    assert!(delete.key.is_empty() && delete.value.is_empty());
    assert_eq!(delete.routing_bucket, write.routing_bucket);
}

#[test]
fn a_damaged_capture_is_refused_or_visibly_shorter_and_never_read_as_the_same_records() {
    // Every byte of the header struct and proto, and of the one record, in the smallest capture.
    let bytes = capture!("hash_page");
    let whole = read("hash_page", &bytes);
    let header_end = 8 + u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let data_start = crate::stream_format::header_block_size(header_end - 8, 0);
    let record_end = data_start + 5 + whole.records[0].payload.len();
    let mut refused = 0;
    let mut shorter = 0;
    for at in (0..header_end).chain(data_start..record_end) {
        let mut damaged = bytes.clone();
        damaged[at] ^= 0x01;
        match read_blob(&damaged) {
            Err(_) => refused += 1,
            Ok(contents) => {
                assert!(
                    contents.records != whole.records,
                    "a flipped bit at byte {at} was read as the same records"
                );
                shorter += 1;
            }
        }
    }
    assert!(refused > 0 && refused + shorter == header_end + (record_end - data_start));
    // Every proper prefix of the file is refused or yields no record; never half of one.
    for length in 0..bytes.len() {
        if let Ok(contents) = read_blob(&bytes[..length]) {
            assert!(contents.records.is_empty(), "a prefix of {length} bytes produced a record");
        }
    }
    // And a page record cut anywhere short is refused by the block codec.
    let payload = &whole.records[0].payload;
    for length in 0..payload.len() {
        assert!(decode_block_image(&payload[..length]).is_err(), "a {length}-byte prefix decoded");
    }
}

fn varint_len(mut value: u64) -> usize {
    let mut n = 1;
    while value >= 0x80 {
        value >>= 7;
        n += 1;
    }
    n
}

/// WHAT EACH LAYER OF THE CAPTURED LAYOUT COSTS AGAINST THE LAYER THIS CRATE WRITES TODAY.
///
/// Print-only. The captured side is the real layout (a record's frame, a page record's header, a
/// file's header block); the local side is the live encoder called on the same inputs. Nothing is
/// pinned, because a literal would go stale on any format change; the point is the table.
#[test]
#[ignore = "print-only measurement"]
fn what_each_layer_costs_here_against_the_captured_layout() {
    // 1. A file's fixed head. The captured streams spend a whole block before the first record.
    let page = capture!("hash_page");
    let proto_size = u32::from_le_bytes(page[0..4].try_into().unwrap()) as usize;
    let captured_head = crate::stream_format::header_block_size(proto_size, 0);
    let ours_head = crate::log_framing::encode_base_header(u64::MAX).len();
    println!();
    println!("  FILE HEAD  captured {captured_head} B (one block); this crate's log piece <= {ours_head} B");

    // 2. A record's frame. Captured: a varint length and a four-byte checksum. Ours: the same, behind
    //    one marker byte -- and a log record carries a payload marker on top of that.
    println!("  RECORD FRAME bytes by payload size: captured | local frame | local frame + payload marker");
    for payload in [50_usize, 200, 1_000, 70_000] {
        let captured = varint_len(payload as u64) + 4;
        let ours = crate::log_framing::encode_frame(&vec![0u8; payload]).len() - payload;
        println!("    {payload:>6}  {captured:>3} | {ours:>3} | {:>3}", ours + 1);
        assert_eq!(ours, captured + 1, "the local frame is the captured one behind a marker byte");
    }

    // 3. A page record against a block record, for the same block bytes.
    println!("  PAGE RECORD vs BLOCK RECORD, bytes spent around the data");
    println!("    key B  data B   captured-layout record   local record   difference");
    for key_len in [4_usize, 24, 64, 128] {
        for data_len in [54_usize, 300, 4_096] {
            let header = crate::block_image::BlockImageHeader {
                routing_bucket: 10_014_346_294_842_661_315,
                block_id: 1,
                timestamp_ms: 1_794_332_653_659,
                key: vec![b'k'; key_len],
                model_id: 3,
                version: 3,
                wal_sequence: 3,
                ..Default::default()
            };
            let data = vec![7u8; data_len];
            let payload = encode_block_image(&header, &data).expect("encodes");
            let captured = varint_len(payload.len() as u64) + 4 + payload.len();
            let ours = crate::block_store::BLOCK_RECORD_HEADER_LEN + data_len;
            println!(
                "    {key_len:>5} {data_len:>7} {captured:>24} {ours:>14} {:>+12}",
                captured as i64 - ours as i64
            );
            assert!(captured > ours, "the captured layout spends more around the data");
        }
    }
    println!();
}

#[test]
fn a_frame_built_here_is_the_frame_in_every_captured_file() {
    use crate::record_framing::frame_payload;
    let mut checked = 0;
    for (name, bytes) in everything() {
        let contents = read(&name, &bytes);
        if !contents.footers.is_empty() {
            // A footer interrupts the run of frames; the writer test covers those files.
            continue;
        }
        let proto_size = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let body = &bytes[crate::stream_format::header_block_size(proto_size, 0)..];
        let mut built = Vec::new();
        for record in &contents.records {
            built.extend_from_slice(&frame_payload(&record.payload));
        }
        assert_eq!(built, body, "{name}: the frames differ from the file's data region");
        checked += 1;
    }
    assert_eq!(checked, 12, "twelve of the fifteen captures have no footer");
}

#[test]
fn a_captured_frame_is_decoded_here_and_a_damaged_or_short_one_is_refused() {
    use crate::record_framing::{decode_framed_at, frame_payload};
    let oplog = capture!("hash_oplog");
    let contents = read("hash_oplog", &oplog);
    let frame = frame_payload(&contents.records[0].payload);
    let (record, next): (WalRecord, usize) = decode_framed_at(&frame, 0).expect("decodes");
    assert_eq!(next, frame.len());
    assert_eq!(record.items[0].object_key, b"key1");
    for at in 0..frame.len() {
        let mut damaged = frame.clone();
        damaged[at] ^= 0x01;
        assert!(decode_framed_at::<WalRecord>(&damaged, 0).is_err(), "a flipped bit at byte {at} was accepted");
    }
    for length in 0..frame.len() {
        assert!(decode_framed_at::<WalRecord>(&frame[..length], 0).is_err(), "a {length}-byte prefix was accepted");
    }
}

#[cfg(feature = "alloc-probe")]
#[test]
fn record_frame_alloc_probe_is_flat_in_the_payload_size() {
    use crate::record_framing::frame_payload;
    for size in [10_usize, 1_000, 100_000] {
        let payload = vec![7u8; size];
        let probe = crate::alloc_probe::Probe::start();
        let framed = frame_payload(&payload);
        let counts = probe.stop();
        std::hint::black_box(&framed);
        assert_eq!(counts.allocs, 1, "framing {size} bytes allocated {} times", counts.allocs);
    }
}
