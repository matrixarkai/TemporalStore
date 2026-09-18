// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scale harness for the raft SNAPSHOT path.
//!
//! Answers, at one corpus size per process so the peak-residency number is attributable:
//!
//!   1. What does building the snapshot cost, and does that cost track the corpus?
//!   2. How many bytes does the snapshot carry, raw and in the encoding the external
//!      publish path actually uses?
//!   3. How many chunks does the chunked install path cut the payload into?
//!   4. Is a writer excluded while the install runs? A prober thread times one propose
//!      taken concurrently with the install; the time it spends blocked is the answer.
//!
//! One size per process on purpose: the peak-residency figure is read from the kernel's
//! own high-water mark, which is reset immediately before the measured region.
//!
//! Run:
//!   cargo run --release --bin snapshot_scale_harness -- --records 8000

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use temporalstore_rust::raft::RaftCluster;
use temporalstore_rust::types::Command;

struct Options {
    records: usize,
    value_bytes: usize,
    chunk_entries: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            records: 8_000,
            value_bytes: 128,
            chunk_entries: 64,
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
            "--value-bytes" => options.value_bytes = value.parse().unwrap_or(options.value_bytes),
            "--chunk-entries" => {
                options.chunk_entries = value.parse().unwrap_or(options.chunk_entries)
            }
            _ => {}
        }
        index += 2;
    }
    options
}

/// Read one size field out of /proc/self/status, in kilobytes.
fn proc_status_kb(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            let digits: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
            return digits.parse().unwrap_or(0);
        }
    }
    0
}

fn rss_kb() -> u64 {
    proc_status_kb("VmRSS:")
}

fn peak_rss_kb() -> u64 {
    proc_status_kb("VmHWM:")
}

/// Reset the kernel's resident high-water mark so the next reading is attributable to the
/// region that follows rather than to the corpus build that preceded it.
fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5\n");
}

fn load_average() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .unwrap_or_default()
        .split_whitespace()
        .take(3)
        .collect::<Vec<_>>()
        .join(" ")
}

fn main() {
    let options = parse_options();
    println!("== snapshot scale harness ==");
    println!("records          {}", options.records);
    println!("value_bytes      {}", options.value_bytes);
    println!("loadavg_at_start {}", load_average());

    let value = vec![b'x'; options.value_bytes];
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);

    let build_started = Instant::now();
    let mut index = 0usize;
    while index < options.records {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: value.clone(),
            })
            .expect("propose must succeed");
        index += 1;
    }
    let corpus_build_ms = build_started.elapsed().as_millis();
    println!("corpus_build_ms  {corpus_build_ms}");
    println!("rss_after_corpus_kb {}", rss_kb());
    println!("loadavg_after_corpus {}", load_average());

    // ---- 1. BUILD -------------------------------------------------------------------
    let rss_before_build_kb = rss_kb();
    reset_peak_rss();
    let create_started = Instant::now();
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let create_ms = create_started.elapsed().as_millis();
    let peak_during_build_kb = peak_rss_kb();
    println!("--- build ---");
    println!("create_snapshot_ms {create_ms}");
    println!("rss_before_build_kb {rss_before_build_kb}");
    println!("peak_rss_during_build_kb {peak_during_build_kb}");
    println!(
        "peak_delta_build_kb {}",
        peak_during_build_kb.saturating_sub(rss_before_build_kb)
    );

    // ---- 2. PAYLOAD -----------------------------------------------------------------
    let entries_carried = snapshot.entries.len();
    let (slab_count, slab_bytes, index_bytes) = match snapshot.state_image.as_ref() {
        Some(image) => (
            image.slabs.len(),
            image.slabs.iter().map(|slab| slab.bytes.len()).sum::<usize>(),
            image.index_bytes.len(),
        ),
        None => (0, 0, 0),
    };
    let raw_payload_bytes = slab_bytes + index_bytes;
    println!("--- payload ---");
    println!("entries_carried  {entries_carried}");
    println!("slab_count       {slab_count}");
    println!("slab_bytes       {slab_bytes}");
    println!("index_bytes      {index_bytes}");
    println!("raw_payload_bytes {raw_payload_bytes}");

    // The encoding the external-store publish path actually serialises with.
    let encode_started = Instant::now();
    let encoded = serde_json::to_vec(&snapshot).expect("snapshot must encode");
    let encode_ms = encode_started.elapsed().as_millis();
    println!("published_encoded_bytes {}", encoded.len());
    println!("encode_ms        {encode_ms}");
    if raw_payload_bytes > 0 {
        println!(
            "encoded_over_raw_x100 {}",
            (encoded.len() as u64 * 100) / raw_payload_bytes as u64
        );
    }
    drop(encoded);

    // ---- 3. CHUNKING ----------------------------------------------------------------
    let chunk_started = Instant::now();
    let chunks = cluster
        .build_install_snapshot_chunks(3, options.chunk_entries)
        .expect("chunking must succeed");
    let chunk_ms = chunk_started.elapsed().as_millis();
    let largest_chunk_state_image_bytes = chunks
        .iter()
        .map(|chunk| match chunk.state_image.as_ref() {
            Some(image) => {
                image.index_bytes.len()
                    + image.slabs.iter().map(|slab| slab.bytes.len()).sum::<usize>()
            }
            None => 0,
        })
        .max()
        .unwrap_or(0);
    println!("--- chunking ---");
    println!("chunk_entries_arg {}", options.chunk_entries);
    println!("chunk_count      {}", chunks.len());
    println!("build_chunks_ms  {chunk_ms}");
    println!("largest_chunk_state_image_bytes {largest_chunk_state_image_bytes}");
    drop(chunks);

    // ---- 4. INSTALL, with a writer probing for exclusion ------------------------------
    // The prober does nothing until the install is in flight, then takes one propose and
    // reports how long that single propose waited. A propose needs the cluster write
    // half, so the wait is exactly the span over which the install excluded a writer.
    let install_running = Arc::new(AtomicBool::new(false));
    let prober_cluster = cluster.clone();
    let prober_flag = Arc::clone(&install_running);
    let prober = std::thread::spawn(move || {
        while !prober_flag.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        // Let the install get properly under way before asking for the lock.
        std::thread::sleep(Duration::from_millis(2));
        let waited = Instant::now();
        let outcome = prober_cluster.propose(Command::StringSet {
            key: "prober-key".to_string(),
            value: b"prober".to_vec(),
        });
        (waited.elapsed().as_millis(), outcome.is_ok())
    });

    let rss_before_install_kb = rss_kb();
    reset_peak_rss();
    install_running.store(true, Ordering::SeqCst);
    let install_started = Instant::now();
    cluster
        .install_snapshot(3, snapshot)
        .expect("install_snapshot must succeed");
    let install_ms = install_started.elapsed().as_millis();
    let peak_during_install_kb = peak_rss_kb();
    let (prober_blocked_ms, prober_ok) = prober.join().expect("prober thread must finish");

    println!("--- install ---");
    println!("install_snapshot_ms {install_ms}");
    println!("rss_before_install_kb {rss_before_install_kb}");
    println!("peak_rss_during_install_kb {peak_during_install_kb}");
    println!(
        "peak_delta_install_kb {}",
        peak_during_install_kb.saturating_sub(rss_before_install_kb)
    );
    println!("concurrent_propose_blocked_ms {prober_blocked_ms}");
    println!("concurrent_propose_ok {prober_ok}");
    println!("loadavg_at_end   {}", load_average());
    println!("== done ==");
}
