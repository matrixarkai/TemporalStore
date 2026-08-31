// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scale harness for WAL reclaim and log-id addressing.
//!
//! Exercises the two properties that block addressing depends on, at a size where a per-record
//! cost would show up:
//!
//!   1. **Log ids survive reclaim.** A block in the WAL is named by the byte offset of its
//!      record. Reclaim compacts the file, so the check is that a log id sampled before a
//!      reclaim still resolves to the same bytes afterwards -- and that a log id whose record
//!      was reclaimed resolves to nothing rather than to whatever now sits at that offset. The
//!      second half matters more: a wrong offset still parses, so it fails silently.
//!
//!   2. **Reclaim cost.** Retained records are copied verbatim rather than decoded and
//!      re-encoded, so reclaiming a large log should cost roughly a file copy rather than a
//!      parse per surviving record.
//!
//! Also checks that sequencing continues across a reopen after reclaim, since the reclaimed
//! file is what the next append reads its starting sequence from.
//!
//! Run:
//!   cargo run --release --bin wal_reclaim_scale_harness -- --records 200000 --reclaims 8

use std::time::Instant;

use temporalstore_rust::types::Command;
use temporalstore_rust::wal::{decode_wal_line, LocalWriteAheadLogStore};

struct Options {
    records: usize,
    reclaims: usize,
    samples: usize,
    value_bytes: usize,
    shard: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            records: 100_000,
            reclaims: 8,
            samples: 512,
            value_bytes: 128,
            shard: 1,
        }
    }
}

fn parse_options() -> Options {
    let mut options = Options::default();
    let args: Vec<String> = std::env::args().collect();
    let mut index = 1;
    while index + 1 < args.len() {
        let value = &args[index + 1];
        match args[index].as_str() {
            "--records" => options.records = value.parse().unwrap_or(options.records),
            "--reclaims" => options.reclaims = value.parse().unwrap_or(options.reclaims),
            "--samples" => options.samples = value.parse().unwrap_or(options.samples),
            "--value-bytes" => options.value_bytes = value.parse().unwrap_or(options.value_bytes),
            "--shard" => options.shard = value.parse().unwrap_or(options.shard),
            _ => {}
        }
        index += 2;
    }
    options
}

/// A log id sampled before any reclaim, with the record bytes it named at the time.
struct Probe {
    log_id: u64,
    sequence: u64,
    bytes: Vec<u8>,
}

fn main() {
    let options = parse_options();
    // A plain directory rather than a temp-dir crate: this is a binary, and the harness wants
    // the path to be inspectable after a failing run anyway.
    let dir = std::env::temp_dir().join("ts-wal-reclaim-scale");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let store = LocalWriteAheadLogStore::new(&dir);
    let shard = options.shard;

    println!("== WAL reclaim scale harness");
    println!(
        "   records={} reclaims={} probes={} value_bytes={}",
        options.records, options.reclaims, options.samples, options.value_bytes
    );

    // ---- append -----------------------------------------------------------
    let payload = vec![b'v'; options.value_bytes];
    let started = Instant::now();
    for index in 0..options.records {
        store
            .append(
                shard,
                Command::StringSet {
                    key: format!("scale-key-{index:09}"),
                    value: payload.clone(),
                },
            )
            .expect("append");
    }
    let append_elapsed = started.elapsed();
    let append_rate = options.records as f64 / append_elapsed.as_secs_f64();
    println!(
        "\n-- append: {} records in {:.2}s ({:.0} rec/s)",
        options.records,
        append_elapsed.as_secs_f64(),
        append_rate
    );

    // ---- sample log ids ---------------------------------------------------
    // scan yields physical offsets; log_id_at converts to the addressing space that survives
    // reclaim. With nothing reclaimed yet the two coincide, which the assertion below pins.
    // Scan in bounded windows and keep only the probes. Materializing the whole log here would
    // make the harness itself the biggest allocation in the process, which is exactly the thing
    // being measured.
    const WINDOW_BYTES: u64 = 4 * 1024 * 1024;
    let mut probes = Vec::new();
    let mut seen = 0usize;
    let mut offset = 0u64;
    let mut stride = 0usize;
    loop {
        let window = store
            .scan(shard, offset, u64::MAX, WINDOW_BYTES)
            .expect("scan");
        if window.is_empty() {
            break;
        }
        if stride == 0 {
            stride = (options.records / options.samples.max(1)).max(1);
        }
        for (physical, line) in &window {
            if seen % stride == 0 {
                let log_id = store.log_id_at(shard, *physical).expect("log_id_at");
                assert_eq!(
                    log_id, *physical,
                    "before any reclaim a log id is the physical offset"
                );
                probes.push(Probe {
                    log_id,
                    sequence: decode_wal_line(line).expect("decode").sequence,
                    bytes: line.clone(),
                });
            }
            seen += 1;
        }
        let (last_offset, last_line) = window.last().expect("non-empty");
        offset = last_offset + last_line.len() as u64;
    }
    assert_eq!(seen, options.records, "scan must see every appended record");
    println!("-- sampled {} log ids across the log", probes.len());

    // ---- reclaim rounds ---------------------------------------------------
    let per_round = options.records / (options.reclaims + 1);
    let mut resolved_ok = 0usize;
    let mut correctly_gone = 0usize;
    let mut wrong_bytes = 0usize;
    let mut resurrected = 0usize;
    let mut total_reclaim = std::time::Duration::ZERO;
    let mut slowest = std::time::Duration::ZERO;

    println!("\n-- reclaim rounds");
    println!(
        "   {:>5} {:>12} {:>10} {:>14} {:>12}",
        "round", "retain_from", "kept", "reclaim_ms", "base_offset"
    );
    for round in 1..=options.reclaims {
        let retain_from = (round * per_round) as u64;
        let started = Instant::now();
        // Measurement harness: no durable index to anchor against, and clamping the pass
        // would measure the clamp rather than the reclaim.
        let durable_index = temporalstore_rust::wal::DurableIndexAnchor::unproven(shard);
        let report = store
            .gc_before_sequence(shard, retain_from, &durable_index)
            .expect("reclaim");
        let elapsed = started.elapsed();
        total_reclaim += elapsed;
        slowest = slowest.max(elapsed);

        println!(
            "   {:>5} {:>12} {:>10} {:>14.1} {:>12}",
            round,
            retain_from,
            report.records_after,
            elapsed.as_secs_f64() * 1000.0,
            report.base_offset
        );

        // Every probe must either resolve to exactly the bytes it named, or resolve to nothing
        // because its record was reclaimed. Anything else is the silent failure this design
        // exists to prevent.
        let base = store.base_offset(shard).expect("base_offset");
        for probe in &probes {
            match store
                .read_at_log_id(shard, probe.log_id, probe.bytes.len() as u64)
                .expect("read_at_log_id")
            {
                Some(bytes) => {
                    if bytes == probe.bytes {
                        resolved_ok += 1;
                    } else {
                        wrong_bytes += 1;
                        if wrong_bytes <= 3 {
                            let found = decode_wal_line(&bytes).map(|record| record.sequence);
                            eprintln!(
                                "   MISMATCH round {round}: log_id {} named sequence {} but now reads {:?}",
                                probe.log_id, probe.sequence, found
                            );
                        }
                    }
                }
                None => {
                    if probe.log_id < base {
                        correctly_gone += 1;
                    } else {
                        // A live log id must never read as absent.
                        resurrected += 1;
                        eprintln!(
                            "   LOST round {round}: log_id {} (sequence {}) is at or above base {base} but did not resolve",
                            probe.log_id, probe.sequence
                        );
                    }
                }
            }
        }
    }

    // ---- reopen -----------------------------------------------------------
    // The reclaimed file is where a fresh process reads its starting sequence. If the header
    // confused that scan, sequencing would restart and silently re-use ids.
    let info_before = store.info(shard).expect("info");
    let reopened = LocalWriteAheadLogStore::new(&dir);
    let appended = reopened
        .append(
            shard,
            Command::StringSet {
                key: "after-reclaim".to_string(),
                value: b"v".to_vec(),
            },
        )
        .expect("append after reopen");

    println!("\n-- reopen after reclaim");
    println!("   last sequence before reopen : {}", info_before.current_sequence);
    println!("   next sequence after reopen  : {}", appended.sequence);
    let sequence_continues = appended.sequence == info_before.current_sequence + 1;

    // ---- verdict ----------------------------------------------------------
    let checks = probes.len() * options.reclaims;
    println!("\n== results");
    println!("   append throughput      : {append_rate:.0} rec/s");
    println!(
        "   reclaim total / slowest: {:.1} ms / {:.1} ms",
        total_reclaim.as_secs_f64() * 1000.0,
        slowest.as_secs_f64() * 1000.0
    );
    println!("   address checks         : {checks}");
    println!("   resolved to same bytes : {resolved_ok}");
    println!("   correctly reclaimed    : {correctly_gone}");
    println!("   WRONG bytes returned   : {wrong_bytes}");
    println!("   live id lost           : {resurrected}");
    println!("   sequence continues     : {sequence_continues}");

    println!("   scratch dir            : {}", dir.display());

    let ok = wrong_bytes == 0
        && resurrected == 0
        && sequence_continues
        && resolved_ok + correctly_gone == checks;
    println!("\n== {}", if ok { "PASS" } else { "FAIL" });
    if !ok {
        std::process::exit(1);
    }
}
