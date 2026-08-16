// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WAL-replay recovery harness. Exercises the relaxed-durability write path (run with
//! TS_INDEXLOG_DEFER_SYNC=1 + TS_WAL_ONLY_SYNC=1, where the served-index delta fdatasync is
//! dropped from the ack path while the WAL + data pages stay durable per write) and, as a
//! stronger stress, proves the WAL alone is self-sufficient: the WAL record embeds the full
//! command payload, so startup replay from the last durable watermark rebuilds the served index
//! and the page state.
//!
//! A plain SIGKILL / process::abort preserves the OS page cache, so un-fsync'd bytes survive it
//! and would not exercise recovery. To faithfully model real power loss this harness also drops
//! state (up to and including the pages, beyond what the relaxed mode actually defers), then
//! asserts that every acked write is reconstructed by WAL replay.
//!
//! Modes:
//!   populate   --root R --keys N [--flush-at F]
//!                write keys 0..N (each ack'd); at write F force a durable dump
//!                (flush_shard_index) and record the durable page-slab lengths; then abort.
//!   powerloss  --root R --scope <wipe-nondurable|truncate-tail|torn-tail>
//!                simulate loss of the never-fsync'd state.
//!   recover    --root R --keys N
//!                load the shard (forces WAL replay) and assert every acked key is present
//!                with the exact value; print a JSON report.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

fn key_of(i: u64) -> String {
    format!("sbk-{i:010}")
}
fn value_of(i: u64) -> Vec<u8> {
    // Non-trivial, sequence-dependent payload so a stale/duplicate page cannot pass.
    format!("sbv-{i:010}-{}", "x".repeat(48)).into_bytes()
}

fn open_engine(root: &Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        256,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    )
}

fn write_key(engine: &TemporalEngine, i: u64) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key_of(i),
            value: value_of(i),
        },
    });
    assert!(response.status.ok, "ack failed for {i}: {}", response.status.message);
}

fn read_key(engine: &TemporalEngine, i: u64) -> Option<Vec<u8>> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key_of(i) },
        })
        .response
    {
        CommandResponse::Bytes { value: Some(bytes) } => Some(bytes),
        _ => None,
    }
}

fn durable_lengths_path(root: &Path) -> PathBuf {
    root.join("durable_lengths.txt")
}

fn record_durable_slab_lengths(root: &Path) {
    let pages = root.join("pages");
    let mut lines = String::new();
    if let Ok(entries) = fs::read_dir(&pages) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("seg") {
                if let Ok(meta) = fs::metadata(&path) {
                    lines.push_str(&format!(
                        "{}\t{}\n",
                        path.file_name().unwrap().to_string_lossy(),
                        meta.len()
                    ));
                }
            }
        }
    }
    fs::write(durable_lengths_path(root), lines).expect("record durable lengths");
}

fn populate(root: PathBuf, keys: u64, flush_at: Option<u64>) -> ! {
    let engine = open_engine(&root);
    engine.load_shard(1);
    for i in 0..keys {
        write_key(&engine, i);
        if Some(i) == flush_at {
            // Background threshold dump: fsync pages + persist the anchored base index, then
            // record the durable frontier (post-dump slab lengths) for the power-loss step.
            engine.flush_shard_index(1);
            record_durable_slab_lengths(&root);
        }
    }
    if flush_at.is_none() {
        // No dump happened: nothing but the WAL is durable. Record empty frontier.
        fs::write(durable_lengths_path(&root), "").expect("record durable lengths");
    }
    // Abrupt process loss AFTER the acks (page/index fsyncs were deferred).
    std::process::abort();
}

fn powerloss(root: PathBuf, scope: &str) {
    match scope {
        // No dump occurred: the ONLY durable state is the fsync'd WAL. Drop everything else,
        // exactly as a power cut would drop the never-fsync'd pages + served index.
        "wipe-nondurable" => {
            let _ = fs::remove_dir_all(root.join("pages"));
            let _ = fs::remove_dir_all(root.join("indexes").join("indexlogs"));
            // Drop any base index snapshot (only the WAL survives a power cut here).
            if let Ok(entries) = fs::read_dir(root.join("indexes")) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with("shard-") && name.ends_with(".index.json") {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
            assert!(
                root.join("indexes").join("wals").exists(),
                "WAL must survive a power cut -- it is the single durable barrier"
            );
        }
        // A dump made pages durable up to the recorded frontier; the post-dump tail pages were
        // appended but never fsync'd. Truncate each slab back to its recorded durable length,
        // modelling loss of exactly the un-synced tail. The (durable) base index + WAL remain.
        "truncate-tail" => {
            let lengths = fs::read_to_string(durable_lengths_path(&root)).unwrap_or_default();
            let mut durable: std::collections::HashMap<String, u64> = Default::default();
            for line in lengths.lines() {
                if let Some((name, len)) = line.split_once('\t') {
                    durable.insert(name.to_string(), len.parse().unwrap_or(0));
                }
            }
            if let Ok(entries) = fs::read_dir(root.join("pages")) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("seg") {
                        continue;
                    }
                    let name = path.file_name().unwrap().to_string_lossy().to_string();
                    let keep = durable.get(&name).copied().unwrap_or(0);
                    let cur = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    if cur > keep {
                        let f = fs::OpenOptions::new().write(true).open(&path).unwrap();
                        f.set_len(keep).expect("truncate un-synced slab tail");
                    }
                }
            }
        }
        // Realistic power loss for the index-log-defer-sync (3->2) mode: the ONLY relaxation is
        // the deferred delta-log fdatasync, so the loss it can cause is an un-synced delta tail.
        // Drop the entire delta index-log (worst case) while the durable pages + WAL + base index
        // remain. Recovery must fold the (durable) base + replay the WAL tail to restore every
        // acked write; the durable pages are never referenced through a lost delta.
        "drop-indexlog" => {
            let _ = fs::remove_dir_all(root.join("indexes").join("indexlogs"));
        }
        // A page write was torn by the crash: lop a few bytes off the newest slab (partial
        // record). Reconcile-on-open must drop the torn record and replay must rebuild it.
        "torn-tail" => {
            let mut newest: Option<(PathBuf, std::time::SystemTime)> = None;
            if let Ok(entries) = fs::read_dir(root.join("pages")) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("seg") {
                        continue;
                    }
                    let m = fs::metadata(&path).and_then(|m| m.modified()).ok();
                    if let Some(m) = m {
                        if newest.as_ref().map(|(_, t)| m > *t).unwrap_or(true) {
                            newest = Some((path, m));
                        }
                    }
                }
            }
            if let Some((path, _)) = newest {
                let len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                if len > 7 {
                    let f = fs::OpenOptions::new().write(true).open(&path).unwrap();
                    f.set_len(len - 7).expect("tear newest slab tail");
                }
            }
        }
        other => panic!("unknown powerloss scope {other}"),
    }
}

#[derive(Serialize)]
struct RecoverReport {
    keys: u64,
    recovered: u64,
    missing: Vec<u64>,
    mismatched: Vec<u64>,
    ok: bool,
}

fn recover(root: PathBuf, keys: u64) {
    let engine = open_engine(&root);
    engine.load_shard(1); // forces WAL replay from the durable watermark
    let mut missing = Vec::new();
    let mut mismatched = Vec::new();
    let mut recovered = 0u64;
    for i in 0..keys {
        match read_key(&engine, i) {
            Some(v) if v == value_of(i) => recovered += 1,
            Some(_) => mismatched.push(i),
            None => missing.push(i),
        }
    }
    let report = RecoverReport {
        keys,
        recovered,
        ok: missing.is_empty() && mismatched.is_empty(),
        missing,
        mismatched,
    };
    println!("{}", serde_json::to_string(&report).expect("serialize report"));
    if !report.ok {
        std::process::exit(2);
    }
}

fn main() {
    let mut root = None;
    let mut mode = None;
    let mut keys = 0u64;
    let mut flush_at: Option<u64> = None;
    let mut scope = String::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => root = args.next().map(PathBuf::from),
            "--mode" => mode = args.next(),
            "--keys" => keys = args.next().and_then(|s| s.parse().ok()).unwrap_or(0),
            "--flush-at" => flush_at = args.next().and_then(|s| s.parse().ok()),
            "--scope" => scope = args.next().unwrap_or_default(),
            other => panic!("unknown argument {other}"),
        }
    }
    let root = root.expect("--root required");
    fs::create_dir_all(&root).expect("create root");
    match mode.as_deref() {
        Some("populate") => populate(root, keys, flush_at),
        Some("powerloss") => powerloss(root, &scope),
        Some("recover") => recover(root, keys),
        other => panic!("--mode must be populate|powerloss|recover, got {other:?}"),
    }
}
