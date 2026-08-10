// Control State scaled-workload harness.
//
// Proves the public Control State use cases (frequency cap, tenant quota, campaign
// pacing, rolling-window control_state suppression) against the in-process TemporalStore
// engine under scaled synthetic load, and reports correctness invariants plus
// latency/throughput/memory metrics.
//
// The workload mirrors the serving shape documented in
// docs/blog_control_state_frequency_caps.md:
//
//     read current state -> apply rule -> atomically update state -> return decision
//
// The atomic increment-then-read primitive is `Command::ControlStateSetAndGet`, the Rust
// analog of the C++ control_state `HSETANDGET` operator. Frequency-cap enforcement
// is validated to be *exact*: for every (user, campaign, day) the number of allowed
// impressions equals min(attempts, cap) with zero violations.
//
// Run:
//   cargo run --release --bin control_state_scale_harness -- \
//       --users 20000 --campaigns 8 --cap 5 --out /tmp/control_state_report.json

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use temporalstore_rust::control::{Config, SetConfigRequest};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::types::{Command, CommandResponse, ControlStateFamily, ExecuteRequest};

const DAY_MS: u64 = 86_400_000;
const MINUTE_MS: u64 = 60_000;
// Fixed synthetic wall-clock anchor (2026-07-24T00:00:00Z in ms) so buckets and
// windows are deterministic and reproducible across runs.
const BASE_DAY_START_MS: u64 = 1_784_851_200_000;

#[derive(Clone)]
struct Options {
    users: u64,
    campaigns: u64,
    cap: i64,
    max_attempts_per_key: u64,
    quota_tenants: u64,
    quota_requests: u64,
    quota_limit: i64,
    pacing_campaigns: u64,
    pacing_events: u64,
    suppression_actors: u64,
    suppression_threshold: i64,
    replay_fraction: u64, // percent of freq-cap events replayed with the same uuid
    seed: u64,
    out: Option<String>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            users: 20_000,
            campaigns: 8,
            cap: 5,
            max_attempts_per_key: 12,
            quota_tenants: 200,
            quota_requests: 300_000,
            quota_limit: 1_000_000,
            pacing_campaigns: 500,
            pacing_events: 200_000,
            suppression_actors: 20_000,
            suppression_threshold: 5,
            replay_fraction: 10,
            seed: 0x5EED_1234,
            out: None,
        }
    }
}

// Small deterministic PRNG (SplitMix64) so scaled runs are reproducible.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn in_range(&mut self, lo: u64, hi_inclusive: u64) -> u64 {
        if hi_inclusive <= lo {
            return lo;
        }
        lo + self.next_u64() % (hi_inclusive - lo + 1)
    }
}

#[derive(Default)]
struct Latency {
    samples: Vec<u64>, // nanoseconds
}
impl Latency {
    fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }
    fn pct(&mut self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples.sort_unstable();
        let idx = (((self.samples.len() - 1) as f64) * p).round() as usize;
        self.samples[idx]
    }
    fn max(&self) -> u64 {
        self.samples.iter().copied().max().unwrap_or(0)
    }
    fn mean(&self) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        (self.samples.iter().map(|v| *v as u128).sum::<u128>() / self.samples.len() as u128) as u64
    }
}

struct ScenarioResult {
    name: String,
    ops: u64,
    elapsed_s: f64,
    throughput: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    mean_us: f64,
    violations: u64,
    notes: Vec<String>,
}

fn as_int(resp: &CommandResponse) -> i64 {
    match resp {
        CommandResponse::Integer { value } => *value,
        other => panic!("expected Integer response, got {other:?}"),
    }
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1000.0
}

// ---- Scenario 1: Frequency cap (headline) --------------------------------
// Exact daily impression cap per (user, campaign). Uses ControlStateSetAndGet as the
// atomic increment-then-read control primitive; allow iff count_after <= cap.
fn scenario_frequency_cap(engine: &TemporalEngine, shard: u64, opt: &Options) -> ScenarioResult {
    let mut rng = Rng(opt.seed ^ 0xF0F0);
    let mut lat = Latency::default();
    let day_start = BASE_DAY_START_MS;
    let day_end = day_start + DAY_MS - 1;

    // Ground-truth expectations per key.
    let mut expected_allows: HashMap<(u64, u64), i64> = HashMap::new();
    let mut observed_allows: HashMap<(u64, u64), i64> = HashMap::new();
    let mut ops: u64 = 0;
    let mut allows: u64 = 0;
    let mut blocks: u64 = 0;
    let mut violations: u64 = 0;

    for u in 0..opt.users {
        for c in 0..opt.campaigns {
            let attempts = rng.in_range(1, opt.max_attempts_per_key);
            let exp = attempts.min(opt.cap as u64) as i64;
            expected_allows.insert((u, c), exp);
            let key = format!("control:t1:user:u{u}:campaign:c{c}:impression:2026-07-24");
            let mut allowed_here = 0i64;
            for a in 0..attempts {
                let ts = day_start + rng.in_range(0, DAY_MS - 1);
                let start = Instant::now();
                let resp = engine
                    .execute(ExecuteRequest {
                        shard_id: shard,
                        command: Command::ControlStateSetAndGet {
                            family: ControlStateFamily::Counter,
                            key: key.clone(),
                            timestamp_ms: ts,
                            amount: 1,
                            start_ms: day_start,
                            end_ms: day_end,
                            aggregator: "sum".to_string(),
                        },
                    })
                    .response;
                lat.record(start.elapsed().as_nanos() as u64);
                let count_after = as_int(&resp);
                ops += 1;
                // Serving decision: allow iff we are within the cap.
                let allow = count_after <= opt.cap;
                if allow {
                    allows += 1;
                    allowed_here += 1;
                    // Invariant: never allow beyond the cap.
                    if allowed_here > opt.cap {
                        violations += 1;
                    }
                } else {
                    blocks += 1;
                    // Invariant: never block while strictly under the cap.
                    if (a as i64) < opt.cap {
                        violations += 1;
                    }
                }
            }
            observed_allows.insert((u, c), allowed_here);
        }
    }

    // Exactness invariant: allowed == min(attempts, cap) for every key.
    for (k, exp) in &expected_allows {
        if observed_allows.get(k).copied().unwrap_or(-1) != *exp {
            violations += 1;
        }
    }

    let elapsed = lat.samples.iter().map(|v| *v as u128).sum::<u128>() as f64 / 1e9;
    let notes = vec![
        format!("keys={} cap={}", expected_allows.len(), opt.cap),
        format!("allow={allows} block={blocks} (allow == Σ min(attempts,cap))"),
        format!(
            "exact_enforcement={}",
            if violations == 0 { "PASS" } else { "FAIL" }
        ),
    ];
    ScenarioResult {
        name: "frequency_cap".to_string(),
        ops,
        elapsed_s: elapsed,
        throughput: ops as f64 / elapsed.max(1e-9),
        p50_us: us(lat.pct(0.50)),
        p95_us: us(lat.pct(0.95)),
        p99_us: us(lat.pct(0.99)),
        p999_us: us(lat.pct(0.999)),
        max_us: us(lat.max()),
        mean_us: us(lat.mean()),
        violations,
        notes,
    }
}

// ---- Scenario 2: Tenant API quota ----------------------------------------
// One control key per tenant/day; weighted increments; allow iff used <= limit.
fn scenario_tenant_quota(engine: &TemporalEngine, shard: u64, opt: &Options) -> ScenarioResult {
    let mut rng = Rng(opt.seed ^ 0xA5A5);
    let mut lat = Latency::default();
    let day_start = BASE_DAY_START_MS;
    let day_end = day_start + DAY_MS - 1;
    let mut ops = 0u64;
    let mut violations = 0u64;
    let mut allows = 0u64;
    let mut blocks = 0u64;
    // Independent ground-truth per tenant.
    let mut truth: HashMap<u64, i64> = HashMap::new();

    for i in 0..opt.quota_requests {
        let tenant = rng.in_range(0, opt.quota_tenants.saturating_sub(1));
        let weight = rng.in_range(1, 5) as i64;
        let ts = day_start + rng.in_range(0, DAY_MS - 1);
        let key = format!("control:t{tenant}:tenant:quota:api_requests:2026-07-24");
        let start = Instant::now();
        let resp = engine
            .execute(ExecuteRequest {
                shard_id: shard,
                command: Command::ControlStateSetAndGet {
                    family: ControlStateFamily::Counter,
                    key,
                    timestamp_ms: ts,
                    amount: weight,
                    start_ms: day_start,
                    end_ms: day_end,
                    aggregator: "sum".to_string(),
                },
            })
            .response;
        lat.record(start.elapsed().as_nanos() as u64);
        let used = as_int(&resp);
        *truth.entry(tenant).or_default() += weight;
        // Decision: allow iff used <= limit.
        if used <= opt.quota_limit {
            allows += 1;
        } else {
            blocks += 1;
        }
        // Invariant: engine's running total exactly equals independent truth.
        if used != *truth.get(&tenant).unwrap() {
            violations += 1;
        }
        ops += 1;
        let _ = i;
    }

    let elapsed = lat.samples.iter().map(|v| *v as u128).sum::<u128>() as f64 / 1e9;
    let notes = vec![
        format!("tenants={} limit={}", truth.len(), opt.quota_limit),
        format!("allow={allows} block={blocks}"),
        format!(
            "running_total_matches_truth={}",
            if violations == 0 { "PASS" } else { "FAIL" }
        ),
    ];
    ScenarioResult {
        name: "tenant_quota".to_string(),
        ops,
        elapsed_s: elapsed,
        throughput: ops as f64 / elapsed.max(1e-9),
        p50_us: us(lat.pct(0.50)),
        p95_us: us(lat.pct(0.95)),
        p99_us: us(lat.pct(0.99)),
        p999_us: us(lat.pct(0.999)),
        max_us: us(lat.max()),
        mean_us: us(lat.mean()),
        violations,
        notes,
    }
}

// ---- Scenario 3: Campaign pacing (read-heavy) ----------------------------
// Spend accrues via increments; a pacing read (ControlStateFamilyQuery sum) computes the
// pace multiplier vs target spend-by-now. Validates read-path latency at scale.
fn scenario_pacing(engine: &TemporalEngine, shard: u64, opt: &Options) -> ScenarioResult {
    let mut rng = Rng(opt.seed ^ 0xC3C3);
    let mut lat = Latency::default();
    let day_start = BASE_DAY_START_MS;
    let day_end = day_start + DAY_MS - 1;
    let mut ops = 0u64;
    let mut violations = 0u64;
    let mut under = 0u64;
    let mut over = 0u64;
    let budget: i64 = 5000;

    for i in 0..opt.pacing_events {
        let c = rng.in_range(0, opt.pacing_campaigns.saturating_sub(1));
        let key = format!("control:t1:campaign:c{c}:pacing:spend:2026-07-24");
        let ts = day_start + rng.in_range(0, DAY_MS - 1);
        // 3:1 read:write mix (pacing is read-heavy).
        let is_read = i % 4 != 0;
        let start = Instant::now();
        let resp = if is_read {
            engine
                .execute(ExecuteRequest {
                    shard_id: shard,
                    command: Command::ControlStateFamilyQuery {
                        family: ControlStateFamily::Counter,
                        key,
                        start_ms: day_start,
                        end_ms: day_end,
                        aggregator: "sum".to_string(),
                    },
                })
                .response
        } else {
            engine
                .execute(ExecuteRequest {
                    shard_id: shard,
                    command: Command::ControlStateSetAndGet {
                        family: ControlStateFamily::Counter,
                        key,
                        timestamp_ms: ts,
                        amount: rng.in_range(1, 20) as i64,
                        start_ms: day_start,
                        end_ms: day_end,
                        aggregator: "sum".to_string(),
                    },
                })
                .response
        };
        lat.record(start.elapsed().as_nanos() as u64);
        let spent = as_int(&resp);
        // Pace decision (target = budget/2 by mid-day, simplified).
        if spent <= budget / 2 {
            under += 1;
        } else {
            over += 1;
        }
        if spent < 0 {
            violations += 1;
        }
        ops += 1;
    }

    let elapsed = lat.samples.iter().map(|v| *v as u128).sum::<u128>() as f64 / 1e9;
    let notes = vec![
        format!("campaigns={} budget={}", opt.pacing_campaigns, budget),
        format!("under_pace={under} over_pace={over} (3:1 read:write)"),
    ];
    ScenarioResult {
        name: "pacing".to_string(),
        ops,
        elapsed_s: elapsed,
        throughput: ops as f64 / elapsed.max(1e-9),
        p50_us: us(lat.pct(0.50)),
        p95_us: us(lat.pct(0.95)),
        p99_us: us(lat.pct(0.99)),
        p999_us: us(lat.pct(0.999)),
        max_us: us(lat.max()),
        mean_us: us(lat.mean()),
        violations,
        notes,
    }
}

// ---- Scenario 4: Rolling-window control_state suppression -------------------------
// Failed-login spikes: increment at minute precision, count over a rolling 60m
// window, suppress when the window count crosses the threshold. Validates that
// suppression triggers exactly at the threshold boundary.
fn scenario_risk_suppression(engine: &TemporalEngine, shard: u64, opt: &Options) -> ScenarioResult {
    let mut rng = Rng(opt.seed ^ 0x7E7E);
    let mut lat = Latency::default();
    let mut ops = 0u64;
    let mut violations = 0u64;
    let mut suppressed_actors = 0u64;
    let base = BASE_DAY_START_MS;

    for actor in 0..opt.suppression_actors {
        // Each actor fires a burst of failed logins within a rolling hour.
        let failures = rng.in_range(1, (opt.suppression_threshold as u64) + 3);
        let key = format!("control:t1:device:d{actor}:login:failed:rolling_1h");
        let window_start = base;
        let mut first_suppress_at: Option<u64> = None;
        for n in 0..failures {
            let ts = base + n * MINUTE_MS + rng.in_range(0, MINUTE_MS - 1);
            let window_end = ts;
            // Atomic: record failure (minute precision) then read rolling count.
            let start = Instant::now();
            let resp = engine
                .execute(ExecuteRequest {
                    shard_id: shard,
                    command: Command::ControlStateIncrementWithOptions {
                        key: key.clone(),
                        timestamp_ms: ts,
                        amount: 1,
                        precision_ms: Some(MINUTE_MS),
                        ttl_ms: Some(DAY_MS),
                    },
                })
                .response;
            lat.record(start.elapsed().as_nanos() as u64);
            let _ = resp;
            let count = as_int(
                &engine
                    .execute(ExecuteRequest {
                        shard_id: shard,
                        command: Command::ControlStateCount {
                            key: key.clone(),
                            start_ms: window_start,
                            end_ms: window_end,
                        },
                    })
                    .response,
            );
            ops += 2;
            let suppressed = count >= opt.suppression_threshold;
            if suppressed && first_suppress_at.is_none() {
                first_suppress_at = Some(n + 1);
            }
            // Invariant: rolling count equals number of failures so far.
            if count != (n as i64 + 1) {
                violations += 1;
            }
        }
        // Invariant: suppression fired iff failures reached the threshold, and at
        // exactly the threshold-th failure.
        let should_suppress = failures as i64 >= opt.suppression_threshold;
        match (should_suppress, first_suppress_at) {
            (true, Some(at)) if at as i64 == opt.suppression_threshold => {
                suppressed_actors += 1;
            }
            (false, None) => {}
            _ => violations += 1,
        }
    }

    let elapsed = lat.samples.iter().map(|v| *v as u128).sum::<u128>() as f64 / 1e9;
    let notes = vec![
        format!(
            "actors={} threshold={}",
            opt.suppression_actors, opt.suppression_threshold
        ),
        format!("suppressed_actors={suppressed_actors}"),
        format!(
            "threshold_exactness={}",
            if violations == 0 { "PASS" } else { "FAIL" }
        ),
    ];
    ScenarioResult {
        name: "risk_suppression".to_string(),
        ops,
        elapsed_s: elapsed,
        throughput: ops as f64 / elapsed.max(1e-9),
        p50_us: us(lat.pct(0.50)),
        p95_us: us(lat.pct(0.95)),
        p99_us: us(lat.pct(0.99)),
        p999_us: us(lat.pct(0.999)),
        max_us: us(lat.max()),
        mean_us: us(lat.mean()),
        violations,
        notes,
    }
}

// ---- Scenario 5: Idempotent replay (at-least-once queue safety) ----------
// Frequency-cap ingestion from an at-least-once queue: a fraction of events are
// redelivered with the SAME uuid. ControlStateSetAndGetWithOptions must dedup
// them so the enforced count matches the count of DISTINCT events, not deliveries.
fn scenario_idempotent_replay(
    engine: &TemporalEngine,
    shard: u64,
    opt: &Options,
) -> ScenarioResult {
    let mut rng = Rng(opt.seed ^ 0x1D1D);
    let mut lat = Latency::default();
    let day_start = BASE_DAY_START_MS;
    let day_end = day_start + DAY_MS - 1;
    let mut ops = 0u64;
    let mut violations = 0u64;
    let mut redeliveries = 0u64;
    let keys = opt.suppression_actors.max(1); // reuse scale knob for distinct keys
    let events_per_key = 6u64;

    for k in 0..keys {
        let key = format!("control:t1:user:iu{k}:campaign:cq:impression:2026-07-24");
        let mut distinct = 0i64;
        let mut last = 0i64;
        for e in 0..events_per_key {
            let uuid = format!("iu{k}-e{e}");
            // Deliver once, then redeliver with the same uuid `replay_fraction`% of the time.
            let deliveries = if rng.in_range(0, 99) < opt.replay_fraction {
                2
            } else {
                1
            };
            distinct += 1;
            for d in 0..deliveries {
                if d > 0 {
                    redeliveries += 1;
                }
                let start = Instant::now();
                last = as_int(
                    &engine
                        .execute(ExecuteRequest {
                            shard_id: shard,
                            command: Command::ControlStateSetAndGetWithOptions {
                                family: ControlStateFamily::Counter,
                                key: key.clone(),
                                timestamp_ms: day_start + e * 1000,
                                amount: 1,
                                start_ms: day_start,
                                end_ms: day_end,
                                aggregator: "sum".to_string(),
                                precision_ms: Some(DAY_MS),
                                ttl_ms: Some(DAY_MS),
                                uuid: Some(uuid.clone()),
                            },
                        })
                        .response,
                );
                lat.record(start.elapsed().as_nanos() as u64);
                ops += 1;
            }
        }
        // Invariant: the enforced count equals DISTINCT events, regardless of replays.
        if last != distinct {
            violations += 1;
        }
    }

    let elapsed = lat.samples.iter().map(|v| *v as u128).sum::<u128>() as f64 / 1e9;
    let notes = vec![
        format!("keys={keys} events_per_key={events_per_key}"),
        format!(
            "redeliveries={redeliveries} deduped (replay_fraction={}%)",
            opt.replay_fraction
        ),
        format!(
            "idempotency_exactness={}",
            if violations == 0 { "PASS" } else { "FAIL" }
        ),
    ];
    ScenarioResult {
        name: "idempotent_replay".to_string(),
        ops,
        elapsed_s: elapsed,
        throughput: ops as f64 / elapsed.max(1e-9),
        p50_us: us(lat.pct(0.50)),
        p95_us: us(lat.pct(0.95)),
        p99_us: us(lat.pct(0.99)),
        p999_us: us(lat.pct(0.999)),
        max_us: us(lat.max()),
        mean_us: us(lat.mean()),
        violations,
        notes,
    }
}

fn parse_args() -> Options {
    let mut opt = Options::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].clone();
        let val = args.get(i + 1).cloned().unwrap_or_default();
        match flag.as_str() {
            "--users" => opt.users = val.parse().unwrap_or(opt.users),
            "--campaigns" => opt.campaigns = val.parse().unwrap_or(opt.campaigns),
            "--cap" => opt.cap = val.parse().unwrap_or(opt.cap),
            "--max-attempts" => {
                opt.max_attempts_per_key = val.parse().unwrap_or(opt.max_attempts_per_key)
            }
            "--quota-tenants" => opt.quota_tenants = val.parse().unwrap_or(opt.quota_tenants),
            "--quota-requests" => opt.quota_requests = val.parse().unwrap_or(opt.quota_requests),
            "--pacing-campaigns" => {
                opt.pacing_campaigns = val.parse().unwrap_or(opt.pacing_campaigns)
            }
            "--pacing-events" => opt.pacing_events = val.parse().unwrap_or(opt.pacing_events),
            "--suppression-actors" => {
                opt.suppression_actors = val.parse().unwrap_or(opt.suppression_actors)
            }
            "--suppression-threshold" => {
                opt.suppression_threshold = val.parse().unwrap_or(opt.suppression_threshold)
            }
            "--seed" => opt.seed = val.parse().unwrap_or(opt.seed),
            "--out" => opt.out = Some(val),
            _ => {}
        }
        i += 1;
    }
    opt
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn emit_json(results: &[ScenarioResult], opt: &Options, total_elapsed_s: f64) -> String {
    let mut s = String::new();
    s.push_str("{\n");
    s.push_str(&format!(
        "  \"config\": {{ \"users\": {}, \"campaigns\": {}, \"cap\": {}, \"quota_tenants\": {}, \"quota_requests\": {}, \"pacing_campaigns\": {}, \"pacing_events\": {}, \"suppression_actors\": {}, \"suppression_threshold\": {}, \"seed\": {} }},\n",
        opt.users, opt.campaigns, opt.cap, opt.quota_tenants, opt.quota_requests,
        opt.pacing_campaigns, opt.pacing_events, opt.suppression_actors,
        opt.suppression_threshold, opt.seed
    ));
    let total_ops: u64 = results.iter().map(|r| r.ops).sum();
    let total_violations: u64 = results.iter().map(|r| r.violations).sum();
    s.push_str(&format!(
        "  \"totals\": {{ \"ops\": {}, \"violations\": {}, \"wall_clock_engine_s\": {:.4}, \"aggregate_throughput_ops_s\": {:.1} }},\n",
        total_ops, total_violations, total_elapsed_s,
        total_ops as f64 / total_elapsed_s.max(1e-9)
    ));
    s.push_str("  \"scenarios\": [\n");
    for (idx, r) in results.iter().enumerate() {
        s.push_str("    {\n");
        s.push_str(&format!("      \"name\": \"{}\",\n", json_escape(&r.name)));
        s.push_str(&format!("      \"ops\": {},\n", r.ops));
        s.push_str(&format!("      \"violations\": {},\n", r.violations));
        s.push_str(&format!("      \"elapsed_s\": {:.4},\n", r.elapsed_s));
        s.push_str(&format!(
            "      \"throughput_ops_s\": {:.1},\n",
            r.throughput
        ));
        s.push_str(&format!("      \"p50_us\": {:.3},\n", r.p50_us));
        s.push_str(&format!("      \"p95_us\": {:.3},\n", r.p95_us));
        s.push_str(&format!("      \"p99_us\": {:.3},\n", r.p99_us));
        s.push_str(&format!("      \"p999_us\": {:.3},\n", r.p999_us));
        s.push_str(&format!("      \"max_us\": {:.3},\n", r.max_us));
        s.push_str(&format!("      \"mean_us\": {:.3},\n", r.mean_us));
        let notes = r
            .notes
            .iter()
            .map(|n| format!("\"{}\"", json_escape(n)))
            .collect::<Vec<_>>()
            .join(", ");
        s.push_str(&format!("      \"notes\": [{}]\n", notes));
        s.push_str(if idx + 1 == results.len() {
            "    }\n"
        } else {
            "    },\n"
        });
    }
    s.push_str("  ]\n}\n");
    s
}

fn main() {
    let opt = parse_args();
    // Back the engine with tmpfs (/dev/shm) storage and async durability — the
    // blog's documented default for high-QPS control state ("Async durability is
    // faster and is the default for high-QPS controls"). This measures the
    // in-memory decision + async-commit hot path rather than per-op fsync latency.
    let storage_root = PathBuf::from(format!(
        "{}/ts_control_state_scale",
        std::env::var("TS_SCALE_STORAGE_ROOT").unwrap_or_else(|_| "/dev/shm".to_string())
    ));
    let _ = std::fs::remove_dir_all(&storage_root);
    let engine = TemporalEngine::with_local_dirs(
        1 << 30, // 1 GiB in-memory cache so the working set stays hot
        storage_root.join("cache"),
        storage_root.join("pages"),
        storage_root.join("indexes"),
    );
    let shard = 1u64;
    engine.load_shard(shard);
    engine.set_config(SetConfigRequest {
        shard_id: shard,
        config: Config {
            async_storage: true,
            ..Config::default()
        },
    });

    println!("== Control State scaled workload ==");
    println!(
        "config: users={} campaigns={} cap={} quota_tenants={} quota_requests={} pacing_events={} suppression_actors={} seed={:#x}",
        opt.users, opt.campaigns, opt.cap, opt.quota_tenants, opt.quota_requests,
        opt.pacing_events, opt.suppression_actors, opt.seed
    );

    let wall = Instant::now();
    let results = vec![
        scenario_frequency_cap(&engine, shard, &opt),
        scenario_tenant_quota(&engine, shard, &opt),
        scenario_pacing(&engine, shard, &opt),
        scenario_risk_suppression(&engine, shard, &opt),
        scenario_idempotent_replay(&engine, shard, &opt),
    ];
    let total_elapsed = wall.elapsed().as_secs_f64();

    println!(
        "\n{:<18} {:>10} {:>12} {:>10} {:>9} {:>9} {:>9} {:>9} {:>10}",
        "scenario", "ops", "ops/s", "mean_us", "p50_us", "p95_us", "p99_us", "p999_us", "viol"
    );
    for r in &results {
        println!(
            "{:<18} {:>10} {:>12.0} {:>10.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>10}",
            r.name,
            r.ops,
            r.throughput,
            r.mean_us,
            r.p50_us,
            r.p95_us,
            r.p99_us,
            r.p999_us,
            r.violations
        );
    }
    let total_ops: u64 = results.iter().map(|r| r.ops).sum();
    let total_viol: u64 = results.iter().map(|r| r.violations).sum();
    println!(
        "\ntotal: {} engine ops, {:.3}s wall, {:.0} ops/s aggregate, {} invariant violations",
        total_ops,
        total_elapsed,
        total_ops as f64 / total_elapsed.max(1e-9),
        total_viol
    );
    for r in &results {
        println!("\n[{}]", r.name);
        for n in &r.notes {
            println!("  - {n}");
        }
    }
    println!(
        "\nCORRECTNESS: {}",
        if total_viol == 0 {
            "ALL INVARIANTS HELD (exact control-state enforcement proven)"
        } else {
            "INVARIANT VIOLATIONS DETECTED"
        }
    );

    if let Some(path) = &opt.out {
        let json = emit_json(&results, &opt, total_elapsed);
        if let Err(e) = std::fs::write(path, json) {
            eprintln!("failed to write {path}: {e}");
        } else {
            println!("\nwrote JSON report to {path}");
        }
    }

    if total_viol != 0 {
        std::process::exit(1);
    }
}
