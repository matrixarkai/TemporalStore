// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one append costs, and what one replay costs, at two log sizes.
//!
//! Counted, not timed. The harness does ONE phase per process so `strace -c` attributes its
//! syscalls to that phase and nothing else -- a process that both writes and reads the log
//! reports one total and no way to split it.
//!
//! The replay phase mirrors the windowed walk in `replay_wal_into_shard_windowed`: the same
//! 512 KiB budget, the same resume point, the same `verify_tail` only on the first window. It
//! stops short of applying the records, because applying them is the engine's cost and not the
//! log's.
//!
//! Run:
//!   wal_cost_scale_harness <root> append <records> <value_bytes>
//!   wal_cost_scale_harness <root> replay

use std::path::PathBuf;

use temporalstore_rust::types::Command;
use temporalstore_rust::wal::LocalWriteAheadLogStore;

/// The same window the engine replays with (`WAL_REPLAY_WINDOW_BYTES`).
const REPLAY_WINDOW_BYTES: u64 = 512 * 1024;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let root = PathBuf::from(args.get(1).expect("usage: <root> <mode> [records] [value_bytes]"));
    let mode = args.get(2).map(|s| s.as_str()).unwrap_or("append");
    let records: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let value_bytes: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(128);

    match mode {
        "append" => append_phase(&root, records, value_bytes),
        "replay" => replay_phase(&root),
        other => panic!("unknown mode {other}"),
    }
}

fn append_phase(root: &PathBuf, records: usize, value_bytes: usize) {
    std::fs::create_dir_all(root).unwrap();
    let store = LocalWriteAheadLogStore::new(root);
    let mut index = 0usize;
    while index < records {
        store
            .append(
                1,
                Command::StringSet {
                    key: format!("k{index:08}"),
                    // Incompressible, and a DIFFERENT payload per record. A repeated byte
                    // compresses to nothing, so a log built from one would hold a tenth of the
                    // bytes its record count suggests -- and the whole point of the large-value
                    // corpus is to move bytes without moving records.
                    value: incompressible(value_bytes, index as u64),
                },
            )
            .unwrap();
        index += 1;
    }
    let stats = store.raw_stats(1);
    println!("PHASE append");
    println!("records          {records}");
    println!("value_bytes      {value_bytes}");
    println!("writes           {}", stats.writes);
    println!("bytes_written    {}", stats.bytes_written);
    println!("syncs            {}", stats.syncs);
    println!("flushes          {}", stats.flushes);
    println!("append_full_scans {}", stats.append_full_scans);
    println!("log_bytes_on_disk {}", dir_bytes(root));
    println!("segment_files    {}", segment_files(root));
}

fn replay_phase(root: &PathBuf) {
    let store = LocalWriteAheadLogStore::new(root);
    // The engine starts past the pieces that hold nothing after its watermark. A watermark of
    // zero means replay everything, which is the restart this measures.
    let start_at = store.log_id_after_sequence(1, 0).unwrap_or(0);
    let mut window_start = start_at;
    let mut verify_tail = true;
    let mut windows = 0u64;
    let mut records = 0u64;
    let mut record_bytes = 0u64;
    loop {
        let (scanned, more_to_come, resume_at) = store
            .scan_decoded_window(1, window_start, REPLAY_WINDOW_BYTES, verify_tail)
            .unwrap();
        verify_tail = false;
        windows += 1;
        for (_, record) in &scanned {
            records += 1;
            record_bytes += record
                .command
                .as_ref()
                .map(|command| match command {
                    Command::StringSet { key, value } => (key.len() + value.len()) as u64,
                    _ => 0,
                })
                .unwrap_or(0);
        }
        if !more_to_come {
            break;
        }
        window_start = resume_at;
    }
    println!("PHASE replay");
    println!("windows          {windows}");
    println!("records_replayed {records}");
    println!("command_bytes    {record_bytes}");
    println!("log_bytes_on_disk {}", dir_bytes(root));
    println!("segment_files    {}", segment_files(root));
}

/// xorshift64*, seeded by length and caller seed so runs stay repeatable.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

fn dir_bytes(root: &PathBuf) -> u64 {
    std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn segment_files(root: &PathBuf) -> usize {
    std::fs::read_dir(root).into_iter().flatten().flatten().count()
}
