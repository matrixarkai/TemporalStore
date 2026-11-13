// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Linux component profile for compact embedding owner searches.
//! cargo run --release -p temporalstore-rust --example embedding_owner_profile -- flat
//! cargo run --release -p temporalstore-rust --example embedding_owner_profile -- owners
//! cargo run --release -p temporalstore-rust --example embedding_owner_profile -- unique-owners
//! cargo run --release -p temporalstore-rust --example embedding_owner_profile -- allocation-only 1
//! Run without alloc-probe; this example supplies its own allocation counter.
//! Synthetic vectors do not establish real-encoder retrieval quality.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use temporalstore_rust::llm_context::{
    EmbeddingIndex, EmbeddingIndexConfig, EmbeddingKey, EmbeddingVector, PreparedEmbeddingQuery,
};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
fn allocated(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
    CALLS.fetch_add(1, Ordering::Relaxed);
}
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            allocated(layout.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            allocated(layout.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(p, layout, size) };
        if !next.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            allocated(size);
        }
        next
    }
}
#[cfg(not(feature = "alloc-probe"))]
#[global_allocator]
static ALLOCATOR: Counting = Counting;

const DIMS: usize = 384;
const ROWS: usize = 100_000;
const QUERIES: usize = 256;

fn vector(id: usize) -> Vec<f32> {
    let mut state = (id as u64).wrapping_add(0x9e3779b97f4a7c15);
    (0..DIMS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 32) as u32 as f64 / u32::MAX as f64 * 2.0 - 1.0) as f32
        })
        .collect()
}

fn cpu_ticks() -> u64 {
    // A zero CPU delta means less than one clock tick, not zero CPU work.
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

fn rss_peak() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next())
                .map(|value| value.parse::<u64>().unwrap() * 1024)
        })
        .unwrap()
}

fn key(id: usize, owner_count: usize) -> EmbeddingKey {
    EmbeddingKey {
        tenant_id: 7,
        owner_id: (id % owner_count) as u64,
        item_id: id as u64,
    }
}

fn allocation_only(rows_per_tenant: usize) {
    assert!(rows_per_tenant > 0 && rows_per_tenant <= ROWS);
    let base = LIVE.load(Ordering::Relaxed);
    let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(DIMS, ROWS)).unwrap();
    let values = [1.0; DIMS];
    for start in (0..ROWS).step_by(512) {
        let end = (start + 512).min(ROWS);
        let batch: Vec<_> = (start..end)
            .map(|id| EmbeddingVector {
                key: EmbeddingKey {
                    tenant_id: 7 + (id / rows_per_tenant) as u64,
                    owner_id: 0,
                    item_id: (id as u64).wrapping_mul(0x9e3779b97f4a7c15),
                },
                values: &values,
            })
            .collect();
        index.upsert_batch(&batch).unwrap();
    }
    let bytes = LIVE.load(Ordering::Relaxed).saturating_sub(base);
    println!("{{\"allocation_only\":true,\"rows\":{ROWS},\"dimensions\":{DIMS},\"rows_per_tenant\":{rows_per_tenant},\"retained_requested_bytes\":{bytes}}}");
    std::hint::black_box(index);
}

fn main() {
    assert!(
        !cfg!(feature = "alloc-probe"),
        "run this profile without alloc-probe"
    );
    let mode = std::env::args().nth(1).unwrap();
    if mode == "allocation-only" {
        let rows = std::env::args()
            .nth(2)
            .map(|value| value.parse().unwrap())
            .unwrap_or(1);
        allocation_only(rows);
        return;
    }
    assert!(mode == "flat" || mode == "owners" || mode == "unique-owners");
    let owner_count = match mode.as_str() {
        "flat" => 1,
        "unique-owners" => ROWS,
        _ => 1_000,
    };
    let clock = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .unwrap();
    assert!(clock.status.success());
    let hz: u64 = std::str::from_utf8(&clock.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    drop(clock);
    let clock_quantum_us = 1_000_000 / hz;
    let base_live = LIVE.load(Ordering::Relaxed);
    let base_calls = CALLS.load(Ordering::Relaxed);
    PEAK.store(base_live, Ordering::Relaxed);
    let build_cpu = cpu_ticks();
    let build_started = Instant::now();
    let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(DIMS, ROWS)).unwrap();
    for start in (0..ROWS).step_by(512) {
        let end = (start + 512).min(ROWS);
        let vectors: Vec<_> = (start..end).map(vector).collect();
        let batch: Vec<_> = vectors
            .iter()
            .enumerate()
            .map(|(offset, values)| EmbeddingVector {
                key: key(start + offset, owner_count),
                values,
            })
            .collect();
        index.upsert_batch(&batch).unwrap();
    }
    let build_us = build_started.elapsed().as_micros();
    let build_cpu_us = (cpu_ticks() - build_cpu) * 1_000_000 / hz;
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(base_live);
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(base_live);
    let build_calls = CALLS.load(Ordering::Relaxed) - base_calls;
    let build_rss_peak = rss_peak();
    println!("{{\"mode\":\"{mode}\",\"rows\":{ROWS},\"dimensions\":{DIMS},\"cpu_clock_quantum_us\":{clock_quantum_us},\"build_us\":{build_us},\"build_cpu_us\":{build_cpu_us},\"retained_requested_bytes\":{retained},\"peak_requested_bytes\":{peak},\"build_alloc_calls\":{build_calls},\"rss_peak_bytes\":{build_rss_peak},\"queries\":[");
    let groups: &[usize] = if mode == "flat" {
        &[1]
    } else {
        &[0, 1, 4, 32, 1_000]
    };
    for (group_index, count) in groups.iter().copied().enumerate() {
        let owners: HashSet<_> = if count == 0 {
            HashSet::from([u64::MAX])
        } else {
            (0..count as u64).collect()
        };
        let check_exact = (mode == "owners" && count <= 4) || (mode == "unique-owners" && count <= 32);
        let mut oracle = EmbeddingIndex::new(EmbeddingIndexConfig::new(DIMS, ROWS)).unwrap();
        if check_exact {
            for id in (0..ROWS).filter(|id| owners.contains(&key(*id, owner_count).owner_id)) {
                let values = vector(id);
                oracle
                    .upsert_batch(&[EmbeddingVector {
                        key: key(id, owner_count),
                        values: &values,
                    }])
                    .unwrap();
            }
        }
        let queries: Vec<_> = (0..QUERIES).map(|id| vector(id * 397 % ROWS)).collect();
        for values in queries.iter().take(8) {
            let query = PreparedEmbeddingQuery::new(values).unwrap();
            std::hint::black_box(index.search_prepared_owners(7, query, 10, &owners).unwrap());
        }
        let mut samples = Vec::with_capacity(QUERIES);
        let mut query_peak = 0usize;
        let mut query_allocs = 0usize;
        let cpu = cpu_ticks();
        let mut digest = 0u64;
        let mut hits_by_query = Vec::with_capacity(QUERIES);
        for values in &queries {
            let query = PreparedEmbeddingQuery::new(values).unwrap();
            let request_live = LIVE.load(Ordering::Relaxed);
            PEAK.store(request_live, Ordering::Relaxed);
            let request_allocs = CALLS.load(Ordering::Relaxed);
            let started = Instant::now();
            let hits = index.search_prepared_owners(7, query, 10, &owners).unwrap();
            samples.push(started.elapsed().as_nanos());
            query_peak = query_peak.max(PEAK.load(Ordering::Relaxed).saturating_sub(request_live));
            query_allocs += CALLS.load(Ordering::Relaxed) - request_allocs;
            assert!(hits.len() <= 10);
            assert!(hits.iter().all(|hit| owners.contains(&hit.key.owner_id)));
            for hit in &hits {
                digest = digest.wrapping_mul(31).wrapping_add(hit.key.item_id);
            }
            hits_by_query.push(hits);
        }
        let cpu_us = (cpu_ticks() - cpu) * 1_000_000 / hz;
        if check_exact {
            for (values, hits) in queries.iter().zip(&hits_by_query) {
                let expected = oracle
                    .search_prepared_owners(
                        7,
                        PreparedEmbeddingQuery::new(values).unwrap(),
                        10,
                        &owners,
                    )
                    .unwrap();
                assert_eq!(*hits, expected, "exact sparse-owner oracle mismatch");
            }
        }
        samples.sort_unstable();
        let p50 = samples[(samples.len() - 1) * 50 / 100];
        let p99 = samples[(samples.len() - 1) * 99 / 100];
        let comma = if group_index + 1 == groups.len() {
            ""
        } else {
            ","
        };
        println!("{{\"owner_count\":{count},\"query_count\":{QUERIES},\"exact_oracle_checked\":{check_exact},\"p50_ns\":{p50},\"p99_ns\":{p99},\"cpu_us\":{cpu_us},\"query_peak_requested_bytes\":{query_peak},\"query_alloc_calls\":{query_allocs},\"digest\":\"{digest}\"}}{comma}");
    }
    println!("]}}");
}
