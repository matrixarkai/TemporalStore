// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Measurement (ignored): what a registry walk costs and what a registering writer waits behind it.
use super::*;


/// MEASUREMENT (ignored): what the registry walk costs, and what a concurrent registering writer
/// waits behind it. `REG_ENTRIES` registrations in one shard, then one `unreferenced_registrations`
/// and one `min_registered_sequence` on the main thread while a second thread registers new blocks
/// in a loop and records its longest single call.
#[test]
#[ignore]
fn what_the_registry_walk_costs_and_what_a_writer_waits() {
    let entries: u64 = std::env::var("REG_ENTRIES").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let dir = tempfile::tempdir().unwrap();
    let blocks = std::sync::Arc::new(crate::block_store::BlockStore::new(dir.path().join("blocks")));
    let wal = std::sync::Arc::new(LocalWriteAheadLogStore::new(dir.path().join("wal")));
    for object in 0..entries {
        register_at(&blocks, 1, object, None, 10, object + 1, &wal);
    }
    // A retained set that keeps one object in ten, so the second pass has a `protected` set to build.
    let retained: std::collections::HashSet<u64> = (0..entries).step_by(10).collect();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (blocks, wal, stop) = (blocks.clone(), wal.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut longest = std::time::Duration::ZERO;
            let mut calls = 0u64;
            let mut object = entries + 1;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let started = std::time::Instant::now();
                register_at(&blocks, 1, object, None, 10, object, &wal);
                longest = longest.max(started.elapsed());
                object += 1;
                calls += 1;
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
            (longest, calls)
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(50));
    let started = std::time::Instant::now();
    let candidates = unreferenced_registrations(&blocks, 1, &wal, &retained);
    let walk = started.elapsed();
    let started = std::time::Instant::now();
    let floor = min_registered_sequence(&blocks, 1, &wal);
    let floor_walk = started.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (longest_wait, calls) = writer.join().unwrap();
    println!(
        "[registry-walk] entries={entries} candidates={} unreferenced_ms={:.1} min_floor_ms={:.1} floor={floor:?} writer_calls={calls} writer_longest_call_ms={:.1}",
        candidates.len(),
        walk.as_secs_f64() * 1e3,
        floor_walk.as_secs_f64() * 1e3,
        longest_wait.as_secs_f64() * 1e3,
    );
}
