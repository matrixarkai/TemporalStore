// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Minimal reclaim cost probe, deliberately using only API that exists on every revision.
//!
//! The full scale harness needs the log-id resolution API, which only exists after that landed,
//! so it cannot run against an older tree. This one uses `append`, `gc_before_sequence` and
//! `info` alone, which lets the same binary be built and run on either revision and the reclaim
//! cost compared directly.

use std::time::Instant;

use temporalstore_rust::types::Command;
use temporalstore_rust::wal::LocalWriteAheadLogStore;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut records = 60_000usize;
    let mut value_bytes = 128usize;
    let mut label = "current".to_string();
    let mut index = 1;
    while index + 1 < args.len() {
        match args[index].as_str() {
            "--records" => records = args[index + 1].parse().unwrap_or(records),
            "--value-bytes" => value_bytes = args[index + 1].parse().unwrap_or(value_bytes),
            "--label" => label = args[index + 1].clone(),
            _ => {}
        }
        index += 2;
    }

    let dir = std::env::temp_dir().join(format!("ts-reclaim-ab-{label}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let store = LocalWriteAheadLogStore::new(&dir);
    let shard = 1u64;

    let payload = vec![b'v'; value_bytes];
    let started = Instant::now();
    for index in 0..records {
        store
            .append(
                shard,
                Command::StringSet {
                    key: format!("ab-key-{index:09}"),
                    value: payload.clone(),
                },
            )
            .expect("append");
    }
    let append_ms = started.elapsed().as_secs_f64() * 1000.0;

    let path = dir.join("shard-1.wal.jsonl");
    let bytes_before = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    // Reclaim the first tenth, which is the shape that matters: a small prefix removed from a
    // large log, so almost everything present has to survive the operation.
    let retain_from = (records / 10) as u64;
    let started = Instant::now();
    // A measurement harness driving reclaim directly: there is no durable index here to
    // anchor against, and constraining the pass would measure the clamp instead of the copy.
    let durable_index = temporalstore_rust::wal::DurableIndexAnchor::unproven(shard);
    let report = store
        .gc_before_sequence(shard, retain_from, &durable_index)
        .expect("reclaim");
    let reclaim_ms = started.elapsed().as_secs_f64() * 1000.0;
    let bytes_after = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    println!(
        "{label}\trecords={records}\tappend_ms={append_ms:.0}\treclaim_ms={reclaim_ms:.1}\tkept={}\tbytes_before={bytes_before}\tbytes_after={bytes_after}",
        report.records_after
    );
    let _ = std::fs::remove_dir_all(&dir);
}
