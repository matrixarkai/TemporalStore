//! feature_aggregate_scale_harness
//!
//! Demonstrates that TemporalStore serves **aggregated features** well over a
//! **high-cardinality** keyspace: the multi-cardinality shape from
//! `docs/blog_feature_sequences_and_aggregates.md`
//! (`user`, `user x category`, `user x author`, `tenant x campaign`).
//!
//! Runs entirely in-process against `TemporalEngine` (no cluster) so it is a
//! deterministic local correctness + perf smoke:
//!
//!   1. Ingest         -- append timestamped observations across N distinct
//!                        FeatureAggregate keys (async durability), then drain.
//!   2. Cold sweep     -- one serving-time FeatureAggQuery per key across the
//!                        whole high-cardinality keyspace; verify every result
//!                        against an in-harness ground truth.
//!   3. Hot serving    -- repeated aggregates over a bounded active working set
//!                        (the realistic online pattern: recent entities are
//!                        hot), reporting steady-state p50/p95/p99 + QPS.
//!
//! Any aggregate that disagrees with the ground truth is a hard failure (the
//! process exits non-zero), so a green run is also an exactness proof.
//!
//! Usage:
//!   cargo run --release -p temporalstore-rust --bin feature_aggregate_scale_harness -- \
//!       --users 4000 --categories 8 --authors 64 --obs-per-key 8 \
//!       --campaigns 200 --campaign-obs 300 --hot-keys 512 --rounds 40

use std::collections::HashMap;
use std::time::{Duration, Instant};

use temporalstore_rust::control::{Config, SetConfigRequest};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::types::{Command, CommandResponse, ExecuteRequest, FeaturePoint};

const SHARD_ID: u64 = 1;
const TS_STEP_MS: u64 = 1_000;

#[derive(Clone)]
struct Options {
    users: u64,
    categories: u64,
    authors: u64,
    obs_per_key: u64,
    campaigns: u64,
    campaign_obs: u64,
    hot_keys: usize,
    rounds: usize,
    settle_ms: u64,
    cold_sample: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            users: 4_000,
            categories: 8,
            authors: 64,
            obs_per_key: 8,
            campaigns: 200,
            campaign_obs: 300,
            hot_keys: 512,
            rounds: 40,
            settle_ms: 1_500,
            cold_sample: usize::MAX, // sweep every key by default
        }
    }
}

/// Deterministic xorshift64 so runs are reproducible without a rand dependency.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo + 1)
    }
}

/// Ground-truth aggregates for one key, computed independently of the engine.
#[derive(Default)]
struct Truth {
    values: Vec<i64>, // in timestamp order
}
impl Truth {
    fn count(&self) -> i64 {
        self.values.len() as i64
    }
    fn sum(&self) -> i64 {
        self.values.iter().sum()
    }
    fn last(&self) -> i64 {
        self.values.last().copied().unwrap_or_default()
    }
}

struct Latencies(Vec<u64>);
impl Latencies {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn record(&mut self, us: u64) {
        self.0.push(us);
    }
    fn pct(&self, p: f64) -> u64 {
        if self.0.is_empty() {
            return 0;
        }
        let pos = p * (self.0.len() - 1) as f64;
        self.0[(pos + 0.5) as usize]
    }
    fn report(&mut self, system_phase: &str, keys: usize, obs: u64, mismatches: u64) {
        self.0.sort_unstable();
        let total_us: u64 = self.0.iter().sum();
        let n = self.0.len().max(1) as u64;
        let qps = (self.0.len() as f64) / (total_us as f64 / 1e6).max(1e-9);
        println!(
            "{system_phase},{keys},{obs},{},{mismatches},{},{},{},{},{},{}",
            self.0.len(),
            qps as u64,
            total_us / n,
            self.pct(0.50),
            self.pct(0.95),
            self.pct(0.99),
            self.0.last().copied().unwrap_or_default(),
        );
    }
}

fn parse_num(args: &[String], value_index: usize, flag: &str) -> u64 {
    args.get(value_index)
        .unwrap_or_else(|| panic!("missing value for {flag}"))
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("invalid number for {flag}"))
}

fn parse_args() -> Options {
    let mut opts = Options::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0usize;
    while i < args.len() {
        let flag = args[i].clone();
        match flag.as_str() {
            "--users" => opts.users = parse_num(&args, i + 1, &flag),
            "--categories" => opts.categories = parse_num(&args, i + 1, &flag),
            "--authors" => opts.authors = parse_num(&args, i + 1, &flag),
            "--obs-per-key" => opts.obs_per_key = parse_num(&args, i + 1, &flag),
            "--campaigns" => opts.campaigns = parse_num(&args, i + 1, &flag),
            "--campaign-obs" => opts.campaign_obs = parse_num(&args, i + 1, &flag),
            "--hot-keys" => opts.hot_keys = parse_num(&args, i + 1, &flag) as usize,
            "--rounds" => opts.rounds = parse_num(&args, i + 1, &flag) as usize,
            "--settle-ms" => opts.settle_ms = parse_num(&args, i + 1, &flag),
            "--cold-sample" => opts.cold_sample = parse_num(&args, i + 1, &flag) as usize,
            "-h" | "--help" => {
                eprintln!("flags: --users --categories --authors --obs-per-key --campaigns --campaign-obs --hot-keys --rounds --settle-ms");
                std::process::exit(0);
            }
            other => panic!("unknown flag {other}"),
        }
        i += 2;
    }
    opts
}

fn build_key(
    key: String,
    obs: u64,
    rng: &mut Rng,
    truth: &mut HashMap<String, Truth>,
    appends: &mut Vec<(String, Vec<FeaturePoint>)>,
    total: &mut u64,
) {
    let mut points = Vec::with_capacity(obs as usize);
    let entry = truth.entry(key.clone()).or_default();
    for i in 0..obs {
        let metric = rng.range(1, 200) as i64; // e.g. dwell duration
        entry.values.push(metric);
        points.push(FeaturePoint {
            timestamp_ms: (i + 1) * TS_STEP_MS,
            value: metric.to_string().into_bytes(),
        });
        *total += 1;
    }
    appends.push((key, points));
}

fn aggregate(engine: &TemporalEngine, key: &str, start_ms: u64, end_ms: u64, agg: &str) -> i64 {
    let resp = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::FeatureAggQuery {
            key: key.to_string(),
            start_ms,
            end_ms,
            aggregator: agg.to_string(),
            count: None,
        },
    });
    assert!(resp.status.ok, "aggregate failed: {resp:?}");
    match resp.response {
        CommandResponse::Aggregate { value } => value,
        other => panic!("unexpected response for {agg}: {other:?}"),
    }
}

fn main() {
    let opts = parse_args();
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD_ID);
    // High-QPS serving controls use async (non-blocking) durability: commit is
    // not gated on a per-write fsync. version must advance past default (0).
    let applied = engine.set_config(SetConfigRequest {
        shard_id: SHARD_ID,
        config: Config {
            version: 1,
            async_storage: true,
            ..Config::default()
        },
    });
    assert!(applied.ok, "set_config(async_storage) failed: {applied:?}");

    // ---- build the high-cardinality key set + observations -----------------
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut truth: HashMap<String, Truth> = HashMap::new();
    let mut appends: Vec<(String, Vec<FeaturePoint>)> = Vec::new();
    let mut total_obs: u64 = 0;

    for u in 0..opts.users {
        let cat = u % opts.categories;
        let author = rng.range(0, opts.authors.saturating_sub(1));
        build_key(format!("feature:content_interaction:user:{u}"), opts.obs_per_key, &mut rng, &mut truth, &mut appends, &mut total_obs);
        build_key(format!("feature:content_interaction:user:{u}:category:{cat}"), opts.obs_per_key, &mut rng, &mut truth, &mut appends, &mut total_obs);
        build_key(format!("feature:content_interaction:user:{u}:author:{author}"), opts.obs_per_key, &mut rng, &mut truth, &mut appends, &mut total_obs);
    }
    for c in 0..opts.campaigns {
        build_key(format!("feature:campaign_delivery:campaign:{c}"), opts.campaign_obs, &mut rng, &mut truth, &mut appends, &mut total_obs);
    }
    let cardinality = appends.len();
    let end_ms = (opts.campaign_obs.max(opts.obs_per_key) + 1) * TS_STEP_MS;

    // ---- phase 1: ingest, then drain the async write backlog ---------------
    let t0 = Instant::now();
    for (key, points) in &appends {
        let resp = engine.execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::FeatureAppend { key: key.clone(), points: points.clone() },
        });
        assert!(resp.status.ok, "append failed for {key}: {resp:?}");
    }
    let ingest_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ingest_qps = (total_obs as f64) / (ingest_ms / 1000.0).max(1e-9);
    // Let the background async flush settle so the serving phases measure the
    // read path, not write-flush contention.
    std::thread::sleep(Duration::from_millis(opts.settle_ms));

    println!("system,phase,keys,observations,queries,mismatches,qps,avg_us,p50_us,p95_us,p99_us,max_us");
    let mut mismatch_total: u64 = 0;

    // ---- phase 2: cold high-cardinality sweep (one query per key) ----------
    // Sample-limited so runtime stays bounded on slow-disk hosts; correctness
    // is still checked on every swept key.
    let cold_n = opts.cold_sample.min(cardinality);
    for agg in ["count", "sum", "last"] {
        let mut lat = Latencies::new();
        let mut mismatches = 0u64;
        for (key, _) in appends.iter().take(cold_n) {
            let q0 = Instant::now();
            let got = aggregate(&engine, key, 0, end_ms, agg);
            lat.record(q0.elapsed().as_micros() as u64);
            let want = match agg {
                "count" => truth[key].count(),
                "sum" => truth[key].sum(),
                "last" => truth[key].last(),
                _ => unreachable!(),
            };
            if got != want {
                if mismatches < 5 {
                    eprintln!("COLD MISMATCH {agg} key={key} got={got} want={want}");
                }
                mismatches += 1;
            }
        }
        mismatch_total += mismatches;
        lat.report(&format!("TemporalStore,cold_sweep_{agg}"), cold_n, total_obs, mismatches);
    }

    // ---- phase 3: hot working-set serving (repeated queries) ---------------
    let hot: Vec<&String> = appends
        .iter()
        .map(|(k, _)| k)
        .take(opts.hot_keys.min(cardinality))
        .collect();
    // warm the working set (untimed)
    for key in &hot {
        let _ = aggregate(&engine, key, 0, end_ms, "sum");
    }
    let mut hot_lat = Latencies::new();
    let mut hot_mismatch = 0u64;
    for _ in 0..opts.rounds {
        for key in &hot {
            let q0 = Instant::now();
            let got = aggregate(&engine, key, 0, end_ms, "sum");
            hot_lat.record(q0.elapsed().as_micros() as u64);
            if got != truth[*key].sum() {
                hot_mismatch += 1;
            }
        }
    }
    mismatch_total += hot_mismatch;
    hot_lat.report("TemporalStore,hot_serving_sum", hot.len(), total_obs, hot_mismatch);

    // ---- summary ------------------------------------------------------------
    eprintln!();
    eprintln!("distinct FeatureAggregate keys (cardinality): {cardinality}");
    eprintln!("total observations ingested:                  {total_obs}");
    eprintln!("ingest: {ingest_qps:.0} obs/s over {ingest_ms:.1} ms");
    eprintln!("total aggregate mismatches:                   {mismatch_total}");
    if mismatch_total != 0 {
        eprintln!("FAIL: aggregate results disagreed with ground truth");
        std::process::exit(1);
    }
    eprintln!("OK: every high-cardinality aggregate matched the exact ground truth");
}
