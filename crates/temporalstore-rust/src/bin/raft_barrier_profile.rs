// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Reports where a raft write's durability barriers are actually taken.
//!
//! A write costs roughly `barriers x fsync`, and on a three-node cluster it has been measured at
//! about 5.5 barriers against 2.3 for a single node -- so most of the latency is barriers, and the
//! question worth answering is which call sites take them. Twice now that question has been
//! answered by reading the code, a change made on the strength of it, and the change then measured
//! at exactly zero. This prints the split instead of arguing it.
//!
//! Runs in its own process because the counters are process-wide: as a test it would race whatever
//! else the harness scheduled alongside it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::path::PathBuf;

use temporalstore_rust::durability_metrics;
use temporalstore_rust::raft::{AppendEntriesRequest, RaftCluster, RaftConfig, RaftLogEntry};
use temporalstore_rust::Command;

/// A private directory for one scenario. `tempfile` is a dev-dependency, so this bin makes its
/// own; the process id keeps concurrent runs from colliding.
fn scratch(scenario: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("ts-barrier-profile-{}-{scenario}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn writes() -> u64 {
    std::env::var("TS_BARRIER_PROFILE_WRITES")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(50)
}

/// Print one scenario's barrier split, normalised per write. The per-write figure is what
/// compares against the fdatasync/write numbers measured under strace on a real node.
fn report(scenario: &str, per: u64, before: BTreeMap<&'static str, u64>) {
    let after = durability_metrics::snapshot();
    let mut rows: Vec<(&'static str, u64)> = after
        .into_iter()
        .map(|(site, count)| (site, count - before.get(site).copied().unwrap_or(0)))
        .filter(|(_, count)| *count > 0)
        .collect();
    rows.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(right.0)));
    let total: u64 = rows.iter().map(|(_, count)| count).sum();
    println!("\n== {scenario} ==  {per} writes, {total} barriers, {:.3} per write", total as f64 / per as f64);
    for (site, count) in rows {
        println!("   {:<34} {:>7}  {:>7.3}/write", site, count, count as f64 / per as f64);
    }
}

/// Leader side: what one `propose` costs in barriers once it is durable.
fn leader_propose(per: u64) {
    let dir = scratch("leader");
    let cluster =
        RaftCluster::new_single_shard_with_wal(&dir, 1, [1, 2, 3], RaftConfig::default())
            .expect("cluster");
    // A deployed process owns exactly one node while holding a view of the whole cluster, and
    // persists only its own record. Left unset -- the in-process default -- it persists a record
    // per peer instead, so this would report three barriers where a real node takes one, and
    // every number here would describe a topology nobody runs.
    cluster.set_local_node_id(1);
    // Warm up first: the first append creates the file and syncs the parent directory, which is a
    // one-off that would otherwise be smeared across the average.
    cluster
        .propose(Command::StringSet { key: "warmup".into(), value: b"warmup".to_vec() })
        .expect("warmup propose");
    let before = durability_metrics::snapshot();
    for index in 0..per {
        cluster
            .propose(Command::StringSet {
                key: format!("profile-{index:06}"),
                value: format!("value-{index:06}").into_bytes(),
            })
            .expect("propose");
    }
    report("leader propose", per, before);
}

/// Follower side: what accepting one AppendEntries carrying a real entry costs. Measured at about
/// 74% of a three-node write, so this is the half that matters most.
///
/// It must carry entries and advance the indices. An empty heartbeat that repeats the indices
/// changes nothing the node must persist, so the fingerprint skip correctly elides it and the
/// scenario reports zero -- true, but not the number anyone came here for.
fn follower_append(per: u64) {
    let dir = scratch("follower");
    let cluster =
        RaftCluster::new_single_shard_with_wal(&dir, 1, [1, 2, 3], RaftConfig::default())
            .expect("cluster");
    // This scenario plays node 2 -- the follower the requests below are addressed to.
    cluster.set_local_node_id(2);
    let request = |index: u64| AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 2,
        prev_log_index: index,
        prev_log_term: if index == 0 { 0 } else { 1 },
        entries: vec![RaftLogEntry {
            term: 1,
            index: index + 1,
            shard_id: 1,
            command: Command::StringSet {
                key: format!("follower-{index:06}"),
                value: format!("value-{index:06}").into_bytes(),
            },
        }],
        // Commit trails by one, as a real leader's does: it can only report an entry committed
        // once a majority has already acknowledged it.
        leader_commit: index,
    };
    cluster.receive_append_entries(request(0)).expect("warmup append");
    let before = durability_metrics::snapshot();
    for index in 1..=per {
        cluster.receive_append_entries(request(index)).expect("append");
    }
    report("follower append (one entry)", per, before);
}

/// Many writers at once. This is the case that separates "each write pays its own barrier" from
/// "writers that arrive while a flush is in flight ride the next one": barriers per write should
/// FALL as concurrency rises if anything coalesces, and stay flat if nothing does.
fn concurrent_propose(per: u64, threads: u64) {
    let dir = scratch(&format!("concurrent-{threads}"));
    let cluster = Arc::new(
        RaftCluster::new_single_shard_with_wal(&dir, 1, [1, 2, 3], RaftConfig::default())
            .expect("cluster"),
    );
    cluster.set_local_node_id(1);
    cluster
        .propose(Command::StringSet { key: "warmup".into(), value: b"warmup".to_vec() })
        .expect("warmup propose");
    let before = durability_metrics::snapshot();
    let each = per / threads;
    let handles: Vec<_> = (0..threads)
        .map(|thread_index| {
            let cluster = Arc::clone(&cluster);
            std::thread::spawn(move || {
                for index in 0..each {
                    cluster
                        .propose(Command::StringSet {
                            key: format!("c-{thread_index:03}-{index:06}"),
                            value: format!("value-{index:06}").into_bytes(),
                        })
                        .expect("propose");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("writer thread");
    }
    report(&format!("concurrent propose (threads={threads})"), each * threads, before);
}

/// How long replaying a node log takes as it gets longer.
///
/// Reported per record, so shape is obvious: flat means replay is linear in the number of
/// records, doubling means it is quadratic and a long-lived node pays for its whole history every
/// time it restarts.
fn recovery_cost() {
    for records in [2_000u64, 4_000] {
        let dir = scratch(&format!("recovery-{records}"));
        let cluster =
            RaftCluster::new_single_shard_with_wal(&dir, 1, [1, 2, 3], RaftConfig::default())
                .expect("cluster");
        cluster.set_local_node_id(1);
        for index in 0..records {
            cluster
                .propose(Command::StringSet {
                    key: format!("r-{index:07}"),
                    value: b"v".to_vec(),
                })
                .expect("propose");
        }
        let wal = temporalstore_rust::raft::LocalRaftWal::new(&dir);
        let started = std::time::Instant::now();
        let recovered = wal.recover_node(1, 1).expect("recover");
        let elapsed = started.elapsed();
        println!(
            "   replay {records:>5} records: {:>8.1} ms total, {:>7.3} ms/record  ({} valid)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1000.0 / records as f64,
            recovered.valid_records
        );
    }
}

/// What one write costs on disk, and how much of that is the encoding rather than the data.
///
/// Every byte here is paid twice over: once writing it, and again on every replay that has to
/// parse it back. A text encoding spells out every field name on every record.
fn wal_format_cost() {
    let records = 2_000u64;
    let dir = scratch("format");
    let cluster =
        RaftCluster::new_single_shard_with_wal(&dir, 1, [1, 2, 3], RaftConfig::default())
            .expect("cluster");
    cluster.set_local_node_id(1);
    for index in 0..records {
        cluster
            .propose(Command::StringSet {
                key: format!("fmt-{index:06}"),
                value: b"0123456789".to_vec(),
            })
            .expect("propose");
    }
    fn dir_bytes(path: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let Ok(meta) = entry.metadata() else { continue };
                if meta.is_dir() {
                    total += dir_bytes(&entry.path());
                } else {
                    total += meta.len();
                }
            }
        }
        total
    }
    let bytes = dir_bytes(&dir);
    println!(
        "   {records} writes produced {bytes} WAL bytes = {:.1} bytes/write",
        bytes as f64 / records as f64
    );
    println!(
        "   the payload actually written was {} bytes/write (key + value)",
        "fmt-000000".len() + 10
    );

    // How much of a record is structure rather than content: strip everything that is a field
    // name, quote, brace or comma from the encoded form and see what is left.
    let sample = std::fs::read_dir(dir.join("raft-wal"))
        .ok()
        .and_then(|mut entries| entries.find_map(|entry| entry.ok()))
        .map(|entry| entry.path());
    if let Some(sample) = sample {
        fn first_line(path: &std::path::Path) -> Option<Vec<u8>> {
            if path.is_dir() {
                let entries = std::fs::read_dir(path).ok()?;
                for entry in entries.flatten() {
                    if let Some(found) = first_line(&entry.path()) {
                        return Some(found);
                    }
                }
                return None;
            }
            let data = std::fs::read(path).ok()?;
            data.split(|byte| *byte == b'\n')
                .find(|line| !line.is_empty())
                .map(|line| line.to_vec())
        }
        if let Some(line) = first_line(&sample) {
            let structural = line
                .iter()
                .filter(|byte| matches!(byte, b'"' | b'{' | b'}' | b'[' | b']' | b',' | b':'))
                .count();
            println!(
                "   one record is {} bytes, of which {} ({:.0}%) are quotes, braces and separators",
                line.len(),
                structural,
                structural as f64 * 100.0 / line.len() as f64
            );
        }
    }
}

fn main() {
    let per = writes();
    leader_propose(per);
    follower_append(per);
    for threads in [1u64, 4, 16] {
        concurrent_propose(per, threads);
    }
    println!("\n== what one write costs on disk ==");
    wal_format_cost();
    println!("\n== replay cost as the log grows ==");
    recovery_cost();
    println!("\ntotal across scenarios: {}", durability_metrics::report_line());
}
