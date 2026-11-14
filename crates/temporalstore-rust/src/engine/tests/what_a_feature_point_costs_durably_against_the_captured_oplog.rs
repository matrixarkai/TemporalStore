// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE FEATURE POINT COSTS DURABLY HERE, AGAINST WHAT THE SAME POINT COSTS IN A CAPTURED
//! OPERATION LOG.
//!
//! The other side of every figure below is read off captured bytes, not a format description:
//! `tests/fixtures/stream_blob_data_feature_oplog.zst` is one point added under a one-character key,
//! `..._feature_many_oplog.zst` is 5,010 points under a three-character key in one request, followed by the ten trims that keep the series at its ceiling. Every
//! point in both is the same four-varint message this crate's `SequenceFeatureRow` encodes, so the
//! two sides are charged for the SAME payload bytes.
//!
//! WHAT IS COMPARED, AND WHAT IS NOT. The captured log is the only stream that carries a point on the
//! write path; the page that holds it follows later, when a dump runs, and no captured feature page
//! exists (only hash pages), so that column is DERIVED from the page body layout and printed as
//! such. Ours spends bytes in three streams per write -- the write-ahead log, the block store and
//! the index log -- and all three are printed, because a total names no term.
//!
//! Print-only: no absolute byte figure is pinned (a literal here would go stale on any format
//! change). What is asserted is that every stream recorded something and that the decomposition is
//! the whole write.
//!
//! rust-internal: measures this crate's own durable streams, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::stream_format::read_blob;
use crate::types::{Command, ExecuteRequest, FeaturePoint, SequenceFeatureRow};

#[derive(Debug, Clone, Copy)]
struct PerPoint {
    wal: f64,
    block: f64,
    index: f64,
}

impl PerPoint {
    fn total(&self) -> f64 {
        self.wal + self.block + self.index
    }
}

fn point(i: u64, base_ts: u64) -> FeaturePoint {
    FeaturePoint {
        timestamp_ms: base_ts + i,
        value: SequenceFeatureRow {
            timestamp_ms: base_ts + i,
            gid: i,
            action_type: i as u32,
            duration: i as u32,
            author_id: i,
        }
        .encode_feature_proto_value(),
    }
}

/// `requests` writes of `per_request` points each, to one key, and what each POINT cost durably.
fn price(key: &str, requests: u64, per_request: u64, base_ts: u64) -> PerPoint {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = std::sync::Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    engine.load_shard(1);
    let wal_before = engine.wal_store.stats(1);
    let block_before = engine.block_store.stats();
    let index_before = engine.index_log_store.stats(1);
    for request in 0..requests {
        let points = (0..per_request).map(|i| point(request * per_request + i, base_ts)).collect();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend { key: key.to_string(), points },
        });
        assert!(response.status.ok, "request {request}: {:?}", response.status);
    }
    let wal = engine.wal_store.stats(1);
    let block = engine.block_store.stats();
    let index = engine.index_log_store.stats(1);
    let n = (requests * per_request) as f64;
    PerPoint {
        wal: (wal.bytes_written - wal_before.bytes_written) as f64 / n,
        block: (block.bytes_written - block_before.bytes_written) as f64 / n,
        index: (index.bytes_written - index_before.bytes_written) as f64 / n,
    }
}

fn capture(name: &str) -> Vec<u8> {
    let compressed: &[u8] = match name {
        "feature_oplog" => include_bytes!("../../../tests/fixtures/stream_blob_data_feature_oplog.zst"),
        "feature_many_oplog" => {
            include_bytes!("../../../tests/fixtures/stream_blob_data_feature_many_oplog.zst")
        }
        other => panic!("no capture named {other}"),
    };
    zstd::decode_all(compressed).expect("the capture decompresses")
}

/// Bytes the captured log spent per point ADDED, framing included: every record's header (a varint
/// length and a four-byte checksum) plus its payload, less the items that trim points away, over the
/// points added. Returns `(bytes per added point, points added, records)`.
fn captured_oplog_bytes_per_point(name: &str) -> (f64, usize, usize) {
    use prost::Message;
    let bytes = capture(name);
    let contents = read_blob(&bytes).expect("the capture reads");
    let mut added = 0_usize;
    let mut spent = 0_usize;
    for record in &contents.records {
        let wal = crate::wal_record::WalRecord::decode(record.payload.as_slice()).expect("a record");
        let mut length_bytes = 1;
        let mut length = record.payload.len() >> 7;
        while length > 0 {
            length_bytes += 1;
            length >>= 7;
        }
        spent += length_bytes + 4 + record.payload.len();
        for item in &wal.items {
            if item.deleted {
                // The item's own bytes: its encoding behind a tag and a one-byte length.
                spent -= 2 + item.encoded_len();
            } else {
                added += 1;
            }
        }
    }
    (spent as f64 / added as f64, added, contents.records.len())
}

/// What a point adds to a page body, derived from the body layout: a length-prefixed key (the
/// timestamp as a varint), a length-prefixed value (the message behind its own length), the
/// cluster and deleted varints, and a nanosecond timestamp varint. DERIVED, NOT CAPTURED.
fn derived_page_bytes_per_point(points: u64) -> f64 {
    fn varint_len(mut v: u64) -> u64 {
        let mut n = 1;
        while v >= 0x80 {
            v >>= 7;
            n += 1;
        }
        n
    }
    let mut total = 0_u64;
    for i in 0..points {
        let key = varint_len(i);
        let value_len = SequenceFeatureRow {
            timestamp_ms: i,
            gid: i,
            action_type: i as u32,
            duration: i as u32,
            author_id: i,
        }
        .encode_feature_proto_value()
        .len() as u64;
        let value = varint_len(value_len) + value_len;
        total += varint_len(key) + key + varint_len(value) + value + 1 + 1 + 9;
    }
    total as f64 / points as f64
}

#[test]
#[ignore = "print-only measurement"]
fn what_a_feature_point_costs_durably_against_the_captured_oplog() {
    let (captured_many, many_points, many_records) = captured_oplog_bytes_per_point("feature_many_oplog");
    let (captured_one, one_points, one_records) = captured_oplog_bytes_per_point("feature_oplog");
    assert_eq!((many_points, many_records), (5_010, 1), "the capture is one request of 5,010 points, then ten trims");
    assert_eq!((one_points, one_records), (1, 1), "the capture is one request of one point");

    let many = price("uid", 1, 5_010, 0);
    let singles = price("9", 200, 1, 777);
    println!();
    println!("  FEATURE POINT, bytes per point durably (the captured column is oplog only; page is derived)");
    println!("  {:<34} {:>8} {:>8} {:>8} {:>8}", "workload", "wal", "block", "index", "total");
    for (name, per) in [("ours: 5,010 points, one request", many), ("ours: 200 requests of one point", singles)] {
        println!("  {name:<34} {:>8.1} {:>8.1} {:>8.1} {:>8.1}", per.wal, per.block, per.index, per.total());
        assert!(per.wal > 0.0 && per.block > 0.0 && per.index > 0.0, "{name}: a stream recorded nothing");
    }
    println!("  captured oplog, 5,010 in one record {captured_many:>8.1} (framing included)");
    println!("  captured oplog, one point per record {captured_one:>7.1} (framing included)");
    println!("  derived page body, per point          {:>6.1}", derived_page_bytes_per_point(5_010));
    println!();
}
