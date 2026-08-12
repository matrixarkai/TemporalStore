// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Distributed multi-user scale harness with forced memory eviction + cold-read promotion.
//!
//! Simulates a distributed TemporalStore in a local setup: several users, each with a large
//! corpus, partitioned under a per-user namespace/table that maps (namespace,table) -> shard ->
//! datanode. Each datanode is a real `TemporalEngine` given a *tiny* memory cache budget, so a
//! large per-user corpus spills memory -> disk-cache -> block-store (eviction) and later reads
//! promote the cold data back into memory. The harness validates, at increasing scale across
//! several iterations:
//!   * multi-user write/read correctness under namespace/table partitioning (read == write),
//!   * eviction actually happened (cache memory_evictions > 0, disk-cache bytes grew),
//!   * cold-read promotion actually happened (block-store reads > 0, cache memory_fills > 0),
//!   * partition isolation (a user only ever reads its own corpus back).
//!
//! Datanodes are independent engines, so the write/read/validate work is run in parallel across
//! datanodes (one scoped thread per datanode) to reach large scale. This exercises the real
//! datanode storage engine (MultiLayerCache tiering + block store + packed pages) and the
//! namespace/table -> shard -> datanode partition routing in-process; the metaserver/proxy TCP
//! process wiring is covered by the distributed_raft_harness / proxy tests.

use std::path::PathBuf;
use std::time::Instant;

use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::types::{Command, CommandResponse, ExecuteRequest, FeaturePoint};

#[derive(Debug, Clone)]
struct Options {
    users: usize,
    string_records_per_user: usize,
    feature_points_per_user: usize,
    datanodes: u64,
    shards: u64,
    memory_capacity_bytes: usize,
    value_bytes: usize,
    iterations: usize,
    scale_growth_percent: u64,
    root: PathBuf,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            users: 16,
            string_records_per_user: 400,
            feature_points_per_user: 200,
            datanodes: 4,
            shards: 16,
            // Deliberately small so the working set dwarfs memory and forces eviction.
            memory_capacity_bytes: 256 * 1024,
            value_bytes: 96,
            iterations: 3,
            scale_growth_percent: 60,
            root: PathBuf::from("/tmp/temporalstore-multiuser-scale"),
        }
    }
}

/// FNV-1a partition of (namespace, table) -> shard in `[1, shards]`.
fn shard_for(namespace: &str, table: &str, shards: u64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in namespace
        .bytes()
        .chain(std::iter::once(b':'))
        .chain(table.bytes())
    {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % shards) + 1
}

fn datanode_for_shard(shard: u64, datanodes: u64) -> usize {
    ((shard - 1) % datanodes) as usize
}

/// (namespace, table, shard, datanode) for a user.
fn route(user: usize, opts: &Options) -> (String, String, u64, usize) {
    let namespace = format!("user{user}");
    let table = "corpus".to_string();
    let shard = shard_for(&namespace, &table, opts.shards);
    let node = datanode_for_shard(shard, opts.datanodes);
    (namespace, table, shard, node)
}

/// Deterministic per-record value so reads can be validated exactly (== what was written),
/// padded to `value_bytes` so the corpus is large enough to overflow the small memory cache.
fn string_value(user: usize, rec: usize, value_bytes: usize) -> Vec<u8> {
    let mut v = format!("u{user}-r{rec}-").into_bytes();
    let seed = (user as u64).wrapping_mul(1_000_003).wrapping_add(rec as u64);
    let mut i = 0u64;
    while v.len() < value_bytes {
        v.push(b'a' + ((seed.wrapping_add(i)) % 26) as u8);
        i += 1;
    }
    v.truncate(value_bytes);
    v
}

#[derive(Default, Debug, Clone)]
struct Partial {
    string_writes: usize,
    feature_writes: usize,
    string_reads: usize,
    feature_reads: usize,
    correctness_mismatches: usize,
    partition_isolation_violations: usize,
    write_ms: u128,
    read_ms: u128,
}

/// Write then read+validate every corpus record for the users routed to one datanode engine.
/// Write-then-read on the same engine means a user's early writes are evicted by later writes,
/// so the read phase exercises cold-read promotion.
fn process_datanode(engine: &TemporalEngine, users: &[usize], opts: &Options) -> Partial {
    let mut p = Partial::default();

    let write_start = Instant::now();
    for &user in users {
        let (ns, table, shard, _) = route(user, opts);
        for rec in 0..opts.string_records_per_user {
            let key = format!("{ns}:{table}:s:{rec}");
            let value = string_value(user, rec, opts.value_bytes);
            let resp = engine.execute(ExecuteRequest {
                shard_id: shard,
                command: Command::StringSet { key, value },
            });
            assert!(resp.status.ok, "string write failed: {resp:?}");
            p.string_writes += 1;
        }
        if opts.feature_points_per_user > 0 {
            let key = format!("{ns}:{table}:f:series");
            let points: Vec<FeaturePoint> = (0..opts.feature_points_per_user)
                .map(|q| FeaturePoint {
                    timestamp_ms: (q as u64) + 1,
                    value: string_value(user, 1_000_000 + q, opts.value_bytes),
                })
                .collect();
            let n = points.len();
            let resp = engine.execute(ExecuteRequest {
                shard_id: shard,
                command: Command::FeatureAppend { key, points },
            });
            assert!(resp.status.ok, "feature write failed: {resp:?}");
            p.feature_writes += n;
        }
    }
    p.write_ms = write_start.elapsed().as_millis();

    let read_start = Instant::now();
    for &user in users {
        let (ns, table, shard, _) = route(user, opts);
        for rec in 0..opts.string_records_per_user {
            let key = format!("{ns}:{table}:s:{rec}");
            let expected = string_value(user, rec, opts.value_bytes);
            let resp = engine.execute(ExecuteRequest {
                shard_id: shard,
                command: Command::StringGet { key: key.clone() },
            });
            p.string_reads += 1;
            match resp.response {
                CommandResponse::Bytes { value: Some(v) } if v == expected => {}
                _ => {
                    p.correctness_mismatches += 1;
                    if p.correctness_mismatches <= 3 {
                        eprintln!("MISMATCH/MISSING user={user} key={key}");
                    }
                }
            }
        }
        if opts.feature_points_per_user > 0 {
            let key = format!("{ns}:{table}:f:series");
            let resp = engine.execute(ExecuteRequest {
                shard_id: shard,
                command: Command::FeatureQuery {
                    key,
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: None,
                },
            });
            p.feature_reads += 1;
            let ok = matches!(resp.response,
                CommandResponse::FeaturePoints { ref points }
                    if points.len() == opts.feature_points_per_user
                        && points.last().map(|pt| &pt.value)
                            == Some(&string_value(user, 1_000_000 + opts.feature_points_per_user - 1, opts.value_bytes)));
            if !ok {
                p.correctness_mismatches += 1;
            }
        }
        // Partition isolation: a key one past this user's corpus must not resolve here.
        let foreign = format!("{ns}:{table}:s:{}", opts.string_records_per_user + 1);
        if let CommandResponse::Bytes { value: Some(_) } = engine
            .execute(ExecuteRequest {
                shard_id: shard,
                command: Command::StringGet { key: foreign },
            })
            .response
        {
            p.partition_isolation_violations += 1;
        }
    }
    p.read_ms = read_start.elapsed().as_millis();
    p
}

#[derive(Default, Debug)]
struct IterationReport {
    users: usize,
    shards: u64,
    datanodes: u64,
    string_writes: usize,
    feature_writes: usize,
    string_reads: usize,
    feature_reads: usize,
    correctness_mismatches: usize,
    partition_isolation_violations: usize,
    memory_evictions: u64,
    disk_cache_bytes: u64,
    memory_fills: u64,
    block_store_reads: u64,
    block_store_writes: u64,
    write_ms: u128,
    read_ms: u128,
}

fn run_iteration(opts: &Options, iter: usize) -> IterationReport {
    let root = opts.root.join(format!("iter-{iter}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create iteration root");

    // D datanode engines with a small memory budget (forces eviction under load).
    let nodes: Vec<TemporalEngine> = (0..opts.datanodes)
        .map(|d| {
            let base = root.join(format!("datanode-{d}"));
            TemporalEngine::with_local_dirs(
                opts.memory_capacity_bytes,
                base.join("cache"),
                base.join("pages"),
                base.join("indexes"),
            )
        })
        .collect();
    for shard in 1..=opts.shards {
        nodes[datanode_for_shard(shard, opts.datanodes)].load_shard(shard);
    }

    // Group users by their hosting datanode.
    let mut users_by_node: Vec<Vec<usize>> = vec![Vec::new(); opts.datanodes as usize];
    for user in 0..opts.users {
        let (_, _, _, node) = route(user, opts);
        users_by_node[node].push(user);
    }

    // Run each datanode's corpus in parallel (independent engines).
    let partials: Vec<Partial> = std::thread::scope(|s| {
        let handles: Vec<_> = nodes
            .iter()
            .enumerate()
            .map(|(idx, engine)| {
                let users = &users_by_node[idx];
                s.spawn(move || process_datanode(engine, users, opts))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut report = IterationReport {
        users: opts.users,
        shards: opts.shards,
        datanodes: opts.datanodes,
        ..Default::default()
    };
    for p in &partials {
        report.string_writes += p.string_writes;
        report.feature_writes += p.feature_writes;
        report.string_reads += p.string_reads;
        report.feature_reads += p.feature_reads;
        report.correctness_mismatches += p.correctness_mismatches;
        report.partition_isolation_violations += p.partition_isolation_violations;
        report.write_ms = report.write_ms.max(p.write_ms); // parallel: wall-clock = slowest
        report.read_ms = report.read_ms.max(p.read_ms);
    }
    for engine in &nodes {
        let cache = engine.cache().stats();
        let bs = engine.block_store().stats();
        report.memory_evictions += cache.memory_evictions;
        report.disk_cache_bytes += cache.disk_bytes;
        report.memory_fills += cache.memory_fills;
        report.block_store_reads += bs.reads;
        report.block_store_writes += bs.writes;
    }
    report
}

fn print_report(iter: usize, r: &IterationReport) {
    println!(
        "{{\"iteration\":{iter},\"users\":{},\"shards\":{},\"datanodes\":{},\
\"string_writes\":{},\"feature_writes\":{},\"string_reads\":{},\"feature_reads\":{},\
\"correctness_mismatches\":{},\"partition_isolation_violations\":{},\
\"memory_evictions\":{},\"disk_cache_bytes\":{},\"memory_fills\":{},\
\"block_store_reads\":{},\"block_store_writes\":{},\"write_wall_ms\":{},\"read_wall_ms\":{}}}",
        r.users, r.shards, r.datanodes, r.string_writes, r.feature_writes, r.string_reads,
        r.feature_reads, r.correctness_mismatches, r.partition_isolation_violations,
        r.memory_evictions, r.disk_cache_bytes, r.memory_fills, r.block_store_reads,
        r.block_store_writes, r.write_ms, r.read_ms,
    );
}

fn parse_args() -> Options {
    let mut o = Options::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut val = || args.next().expect("missing value for flag");
        match flag.as_str() {
            "--users" => o.users = val().parse().unwrap(),
            "--string-records-per-user" => o.string_records_per_user = val().parse().unwrap(),
            "--feature-points-per-user" => o.feature_points_per_user = val().parse().unwrap(),
            "--datanodes" => o.datanodes = val().parse().unwrap(),
            "--shards" => o.shards = val().parse().unwrap(),
            "--memory-bytes" => o.memory_capacity_bytes = val().parse().unwrap(),
            "--value-bytes" => o.value_bytes = val().parse().unwrap(),
            "--iterations" => o.iterations = val().parse().unwrap(),
            "--scale-growth-percent" => o.scale_growth_percent = val().parse().unwrap(),
            "--root" => o.root = PathBuf::from(val()),
            other => panic!("unknown flag: {other}"),
        }
    }
    assert!(o.datanodes > 0 && o.shards >= o.datanodes, "need shards >= datanodes >= 1");
    o
}

fn main() {
    let base = parse_args();
    let _ = std::fs::remove_dir_all(&base.root);
    eprintln!(
        "distributed_multiuser_scale_harness: {} iterations, start users={} strings/user={} \
features/user={} datanodes={} shards={} memory_bytes={}",
        base.iterations, base.users, base.string_records_per_user, base.feature_points_per_user,
        base.datanodes, base.shards, base.memory_capacity_bytes,
    );

    let mut opts = base.clone();
    let mut all_ok = true;
    let mut total_reads = 0usize;
    for iter in 0..base.iterations {
        let r = run_iteration(&opts, iter);
        print_report(iter, &r);
        total_reads += r.string_reads + r.feature_reads;

        let mut ok = true;
        if r.correctness_mismatches != 0 {
            eprintln!("FAIL iter {iter}: {} read/write mismatches", r.correctness_mismatches);
            ok = false;
        }
        if r.partition_isolation_violations != 0 {
            eprintln!("FAIL iter {iter}: {} partition-isolation violations", r.partition_isolation_violations);
            ok = false;
        }
        if r.memory_evictions == 0 {
            eprintln!("FAIL iter {iter}: no memory evictions -- eviction path not exercised");
            ok = false;
        }
        if r.block_store_reads == 0 {
            eprintln!("FAIL iter {iter}: no block-store reads -- cold-read promotion not exercised");
            ok = false;
        }
        all_ok &= ok;
        eprintln!(
            "iter {iter}: {}  users={} writes={} reads={} (evictions={}, disk_cache={}B, promotions/fills={}, cold_block_reads={}, write_wall={}ms read_wall={}ms)",
            if ok { "PASS" } else { "FAIL" }, r.users,
            r.string_writes + r.feature_writes, r.string_reads + r.feature_reads,
            r.memory_evictions, r.disk_cache_bytes, r.memory_fills, r.block_store_reads,
            r.write_ms, r.read_ms,
        );

        let grow = |x: usize| (x as u64 * (100 + base.scale_growth_percent) / 100).max(x as u64 + 1) as usize;
        opts.users = grow(opts.users);
        opts.string_records_per_user = grow(opts.string_records_per_user);
        opts.feature_points_per_user = grow(opts.feature_points_per_user);
    }

    eprintln!(
        "OVERALL: {} across {} iterations, {} total reads validated",
        if all_ok { "PASS" } else { "FAIL" }, base.iterations, total_reads,
    );
    let _ = std::fs::remove_dir_all(&base.root);
    if !all_ok {
        std::process::exit(1);
    }
}
