// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A READ DOES NOT TAKE THE STORE MUTEX.
//!
//! Every read used to hold the store's one mutex for its whole length -- the file open, the
//! positioned read, the checksum and the decompression -- so a read waited behind whatever else
//! held it (an append, a roll, a collection pass) and made all of them wait behind its own disk
//! time. The root a read needs never changes and the counters are sums, so a read has no use for
//! the mutex at all.
//!
//! THE GUARD holds the mutex on one thread and reads on another. A read that took the mutex
//! cannot finish until the hold is released; one that does not, finishes at once. The control is a
//! call that DOES need the mutex, which must stay blocked for the same hold -- without it, a hold
//! that held nothing would pass the guard.
//!
//! THE MEASUREMENT (ignored by default, `--ignored --nocapture`) runs readers while a writer
//! appends and rolls, and prints the reads' latency percentiles. It uses only the public API, so
//! the same file measures the store before and after the read path changed.
use super::*;
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[test]
fn reads_complete_while_another_thread_holds_the_store_mutex() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let first = store.append(b"first-record").unwrap();
    let second = store.append(b"second-record").unwrap();
    let slab_id = first.block_slab_id();

    // The control: a roll needs the mutex, so while it is held the roll must stay blocked.
    let held = store.inner.lock().expect("store mutex");
    let (roll_tx, roll_rx) = mpsc::channel();
    let roller = {
        let store = store.clone();
        std::thread::spawn(move || {
            roll_tx.send(store.roll_slab().is_ok()).ok();
        })
    };
    assert!(
        roll_rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "control: a roll finished while the store mutex was held, so the hold held nothing and \
         the reads below prove nothing"
    );

    // The reads: each of the three read entry points, on its own thread, mutex still held.
    let (tx, rx) = mpsc::channel();
    let reader = {
        let store = store.clone();
        std::thread::spawn(move || {
            let record = store.read(&first).expect("read");
            let range = store
                .read_range(slab_id, second.offset(), second.length())
                .expect("read_range");
            let slab = store.read_slab(slab_id).expect("read_slab");
            tx.send((record, range, slab)).ok();
        })
    };
    let outcome = rx.recv_timeout(Duration::from_secs(10));
    drop(held);
    reader.join().expect("reader thread");
    roller.join().expect("roller thread");
    assert!(
        roll_rx.recv_timeout(Duration::from_secs(10)).expect("the roll finishes once the hold is released"),
        "the roll failed"
    );
    let (record, range, slab) = outcome.unwrap_or_else(|_| {
        panic!("a read waited on the store mutex for 10 s while another thread held it")
    });
    assert_eq!(record, b"first-record");
    assert!(!range.is_empty());
    assert!(slab.len() >= record.len());
}

#[test]
fn the_read_counters_still_add_up_without_the_mutex() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let address = store.append(b"counted").unwrap();
    let before = store.stats();
    for _ in 0..5 {
        assert_eq!(store.read(&address).unwrap(), b"counted");
    }
    let after = store.stats();
    assert_eq!(after.reads, before.reads + 5, "every read is counted exactly once");
    assert!(after.bytes_read > before.bytes_read);
    assert_eq!(
        after.logical_bytes_read,
        before.logical_bytes_read + 5 * b"counted".len() as u64
    );
}

/// Read latency while a writer appends and rolls. Prints, asserts nothing.
#[test]
#[ignore]
fn read_latency_while_a_writer_holds_the_store_mutex() {
    let secs: u64 = std::env::var("BENCH_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(5);
    let readers: usize = std::env::var("BENCH_READERS").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let mut addresses = Vec::new();
    for round in 0..8 {
        for k in 0..250 {
            let mut payload = vec![(round * 31 + k) as u8; 4096];
            payload[0] = (k % 251) as u8;
            addresses.push(store.append(&payload).unwrap());
        }
        store.roll_slab().unwrap();
    }
    let addresses = std::sync::Arc::new(addresses);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let store = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let (mut appends, mut rolls) = (0u64, 0u64);
            let payload = vec![7u8; 64 * 1024];
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                for _ in 0..16 {
                    store.append(&payload).unwrap();
                    appends += 1;
                }
                store.roll_slab().unwrap();
                rolls += 1;
            }
            (appends, rolls)
        })
    };
    let handles: Vec<_> = (0..readers)
        .map(|t| {
            let store = store.clone();
            let addresses = addresses.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut latencies = Vec::new();
                let mut i = t * 97;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let address = &addresses[i % addresses.len()];
                    let started = Instant::now();
                    store.read(address).expect("read");
                    latencies.push(started.elapsed().as_nanos() as u64);
                    i += 1;
                }
                latencies
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (appends, rolls) = writer.join().unwrap();
    let mut all: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort_unstable();
    let pct = |p: f64| all[((all.len() as f64 - 1.0) * p) as usize] as f64 / 1000.0;
    eprintln!(
        "BENCH readers={readers} secs={secs} reads={} reads/s={:.0} p50={:.1}us p99={:.1}us p999={:.1}us max={:.1}us | writer appends={appends} rolls={rolls}",
        all.len(),
        all.len() as f64 / secs as f64,
        pct(0.50),
        pct(0.99),
        pct(0.999),
        *all.last().unwrap() as f64 / 1000.0
    );
}
