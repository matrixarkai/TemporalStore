// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! How long the data server takes to serve one write, over a real HTTP round trip.
//!
//! # WHY THIS EXISTS
//!
//! Every other cost harness in this crate counts BYTES. None of them times a REQUEST, so the
//! only latency figures available were in-process engine calls, which skip the listener, the
//! JSON decode, the response encode and the socket entirely. This drives `POST /execute` over
//! loopback against a running server, which is the thing an operator's client actually waits on.
//!
//! # THE PROFILE IS THE RESULT, AND THIS HARNESS REFUSES TO FORGET IT
//!
//! A debug build of this server was measured at **27.48 ms** per write where the release build
//! of the same commit measured **4.74 ms** -- a 5.8x swing from `-O0` alone, larger than any
//! difference this harness is likely to be pointed at. A figure taken against a debug server is
//! therefore not a latency figure, it is a measurement of the optimiser being off.
//!
//! The crate-wide rule that correctness tests may run in debug (because an assertion's verdict
//! does not move with the optimiser) MUST NOT be read as licence to time a debug binary. Here
//! the profile IS the quantity. So this harness refuses to print a figure when it was itself
//! compiled without optimisation, and `--allow-debug` only downgrades that to a stamp on every
//! line, never to silence.
//!
//! It checks its OWN profile, not the server's, which is a real limitation and is stated in the
//! output: a release harness pointed at a debug server cannot tell. Build and run both from the
//! same checkout, which is what the usage line says to do.
//!
//! # BRINGING A SERVER UP TO POINT THIS AT
//!
//! `TS_META_ADDR=""` starts the data server STANDALONE -- it logs "standalone datanode: no
//! metaserver -- serving shard locally" and serves shard 1 immediately, with no metaserver, no
//! namespace and no table creation. That one variable is the whole bring-up:
//!
//! ```text
//! TS_SERVER_BIND_ADDR=127.0.0.1:17102 TS_SHARD_ID=1 TS_META_ADDR="" \
//! TS_CACHE_DIR=/tmp/h/cache TS_BLOCK_STORE_DIR=/tmp/h/blocks TS_INDEX_DIR=/tmp/h/indexes \
//!   ./target/release/server
//! ```
//!
//! # THE BODY ENCODING, SETTLED BY ASKING THE SERVER RATHER THAN READING THE CODEC
//!
//! `Command::StringSet`'s value is a byte string, and `/execute` accepts BOTH spellings of it in
//! JSON -- a base64 string and a byte array -- and reads back identically either way. That was
//! established by posting both and comparing the read-back, not by deriving it from the
//! serializer, so nobody needs to re-derive it. This harness sends the typed `Command`, so serde
//! picks the spelling and the question does not arise again here.
//!
//! # IF YOU EVER COMPARE TWO SERVERS, INTERLEAVE OP FOR OP
//!
//! Not needed for one arm, recorded because it is the method and this is where the tool lives.
//! Two sequential runs on a shared box differ by whatever the load did between them. Alternating
//! one op on each arm makes any drift hit both equally, and it is the difference between a
//! comparison and two unrelated numbers. The loadavg stamped on every figure below is what makes
//! a later reader able to tell whether a run is even worth comparing.
//!
//! # WHAT IS REPORTED, AND WHY NOT JUST A MEAN
//!
//! p50, p90, p99 and max beside the mean, because the tail is where server differences actually
//! show up: two servers have been seen to agree on the mean to within 2.5% while differing by
//! 40% at the maximum. A mean alone hides exactly the part worth looking at.
//!
//! # READS ARE A DIFFERENT MEASUREMENT AND NEED A PREFILL
//!
//! `--op=get` and `--op=hget` time a READ, which means the keys have to exist first. The prefill
//! is written before the clock starts and is NEVER included in the per-op figures -- a read
//! benchmark that pays for its own writes is measuring a mixed workload and reporting it as a
//! read. The prefill is also where arrival is proven for a read run: if the prefill's own
//! read-back fails there is nothing to read and the figures would be timing 404s, which a status
//! code alone would not reveal.
//!
//! A read run also asserts the response CARRIED A VALUE, not merely that the server answered.
//! Timing a miss is the read-side version of timing a write that stored nothing.
//!
//! # FEATURE AND SEQUENCE, AND THE DIMENSION THAT ACTUALLY DRIVES THEM
//!
//! Every cost figure this crate had for the feature model counted BYTES. None timed one. The
//! arms below close that, and they are swept on the dimension a prior serving measurement says
//! matters: **read cost is first-order in FIELD COUNT, second-order in bytes.** A 1,035 B/row
//! cell carried 36,930 more response bytes than the absent-key floor and still cost LESS than a
//! 20 B/row cell (+604.9 us against +731.8 us), because it held 60 fat field slots against 110
//! thin ones. So a view of many narrow features is the expensive shape, and a sweep over total
//! bytes alone would reproduce the very error that finding corrects.
//!
//! Hence `--points` (how many points one append carries) is a SEPARATE axis from `value_bytes`
//! (how wide each point is). Hold their product fixed and vary the split to test the model.
//!
//! **A sequence row has no width axis.** `SequenceFeatureRow` is five fixed-width scalars
//! (`timestamp_ms`, `gid`, `action_type`, `duration`, `author_id`), so `--rows` is a count axis
//! and `value_bytes` does not apply to it. That is a property of the schema, not an omission
//! here, and the output says so rather than printing a width that did nothing.
//!
//! # THE PAYLOAD IS INCOMPRESSIBLE BY DEFAULT ON THE NEW ARMS
//!
//! `wal_compress_records` defaults ON, so an all-identical payload does not measure a real
//! write. The same `vec![b'v'; n]` fixture agreed to the decimal at 64 B and was **13.51x**
//! wrong at 1024 B elsewhere in this crate. The feature and sequence arms therefore fill values
//! from a seeded xorshift by default; `--payload=identical` is kept as an explicit control, and
//! the spelling is printed on every run so no figure is ambiguous about which it was.
//!
//! The four original arms keep their historical `vec![b'v'; n]` payload so figures already
//! published against them stay comparable; `--payload` overrides it on any arm.
//!
//! # CONCURRENCY IS SWEPT, NOT ASSUMED
//!
//! A single-concurrency latency figure misleads whoever reads it. Prior serving work found reads
//! peaking at concurrency 4 and never improving, with p99 growing 12x from 4 to 16, and durable
//! commit peaking a step earlier at 2 with its tail collapsing to p99 = 1.22 s at 16. So
//! `--concurrency=N` runs N threads over DISJOINT key sets (thread t takes i % N == t), which
//! measures the server under parallel arrival rather than lock contention on one key.
//!
//! Throughput is reported beside the percentiles, because a p50 that improves while ops/s falls
//! is a worse server and the percentiles alone will not say so.
//!
//! # THE INSTRUMENT'S OWN CEILING: ONE TCP CONNECTION PER OP
//!
//! `post_json` opens a fresh connection for every request, so a run leaves one socket per op in
//! `TIME_WAIT` for the kernel's timeout. Back-to-back runs therefore accumulate them, and the
//! next run dies with **`EAGAIN` -- "Resource temporarily unavailable" (os error 11)** when the
//! local ephemeral port range is exhausted. Measured here: a 1,200-op read run failed at prefill
//! index 23 while a 1,200-op WRITE run of the same data volume on the same server passed
//! 1,200/1,200 moments earlier. The variable was elapsed time since the previous run, not load,
//! not volume, and not anything the server did.
//!
//! **That is a limit of this harness, not of the store, and a figure taken through it is not a
//! server figure.** Two consequences, both load-bearing:
//!
//! - Keep per-run `ops` modest (a few hundred) and leave a few seconds between runs. Raising
//!   `ops` to beat sampling noise BACKFIRES: it buys per-op samples and spends socket headroom,
//!   and a 2,000-op sweep here produced p50 spreads of 280-490% where the same cells at 240 ops
//!   spread 12-55%.
//! - Average over more REPEATS, not more ops, when a contrast needs resolving.
//!
//! `EAGAIN` is called out by name in the failure line rather than being reported as a transport
//! error like any other, because it reads exactly like a server refusing load and it is not.
//!
//! # LOAD AVERAGE IS NOT A CONTENTION SIGNAL ON THIS CLASS OF BOX
//!
//! The loadavg stamp below is retained for continuity, but it must not be used to decide whether
//! a run is comparable. It has been observed at **10,206 on a healthy box** -- 19 GB free, no
//! OOM, I/O pressure 5%, and ZERO tasks in D state -- because each cross-boundary helper process
//! counts toward it. Every "taken at load N" caveat written against this harness was reading a
//! meaningless number. `io_pressure_some_avg10`, `mem_available_kb` and `d_state_tasks` below
//! are the real signals; prefer them.
//!
//! Run:
//!   data_server_write_latency_harness <addr> [ops] [key_bytes] [value_bytes] [sleep_ms]
//!       [--op=set|get|hset|hget|featappend|featquery|seqadd|seqquery]
//!       [--points=N] [--rows=N] [--concurrency=N] [--payload=random|identical] [--allow-debug]
//!   data_server_write_latency_harness 127.0.0.1:17102 200 16 64 10 --op=get
//!   data_server_write_latency_harness 127.0.0.1:17102 400 16 32 0 --op=featappend --points=16
//!   data_server_write_latency_harness 127.0.0.1:17102 400 16 32 0 --op=featquery --concurrency=4

use temporalstore_rust::http::post_json;
use temporalstore_rust::types::{
    Command, CommandResponse, ExecuteRequest, ExecuteResponse, FeaturePoint, SequenceFeatureRow,
};

/// Nearest-rank percentile over an ALREADY SORTED slice.
///
/// Nearest-rank rather than interpolating, so every reported figure is a sample that actually
/// occurred -- an interpolated "p99" of a 200-op run is a number no request ever took. Clamped
/// at the top because `(len * 100) / 100` indexes one past the end.
fn percentile_sorted(sorted: &[f64], pct: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (sorted.len() * pct).div_ceil(100);
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

/// The 1-minute load average, stamped on every figure. A latency number without the load it was
/// taken at is not reproducible on a shared machine.
fn loadavg_1m() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// The share of the last 10 s in which at least one task was stalled on I/O.
///
/// THIS, not the load average, is the contention signal on this class of box. See the module
/// note: loadavg has read 10,206 while 19 GB were free and nothing was in D state.
fn io_pressure_some_avg10() -> String {
    std::fs::read_to_string("/proc/pressure/io")
        .ok()
        .and_then(|text| {
            text.lines().find(|l| l.starts_with("some")).and_then(|l| {
                l.split_whitespace()
                    .find_map(|f| f.strip_prefix("avg10=").map(str::to_string))
            })
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// `MemAvailable` in kB -- what actually binds on this box, where a release link wants ~5 GB.
fn mem_available_kb() -> String {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|l| l.starts_with("MemAvailable:"))
                .and_then(|l| l.split_whitespace().nth(1).map(str::to_string))
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Count of tasks in uninterruptible sleep. A genuine I/O stall has some; a five-figure load
/// average with zero of these is an artefact of the process count, not contention.
fn d_state_tasks() -> usize {
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return 0;
    };
    dir.filter_map(Result::ok)
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.bytes().all(|b| b.is_ascii_digit())))
        .filter(|e| {
            std::fs::read_to_string(e.path().join("stat")).is_ok_and(|st| {
                // Field 3 is the state letter, after `pid (comm)`. `comm` can itself contain
                // spaces and parentheses, so split on the LAST ')' rather than on whitespace.
                st.rsplit_once(')')
                    .and_then(|(_, rest)| rest.split_whitespace().next())
                    .is_some_and(|state| state.starts_with('D'))
            })
        })
        .count()
}

/// A deterministic, INCOMPRESSIBLE fill.
///
/// `wal_compress_records` is on by default, so an all-identical value measures the compressor
/// rather than the write. xorshift64* keyed by `seed` gives a per-key distinct, run-to-run
/// reproducible body that zstd cannot shrink -- reproducible matters because a latency cell is
/// run three times and the three runs must differ only in timing.
fn incompressible(seed: u64, n: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(n + 8);
    while out.len() < n {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        out.extend_from_slice(&state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes());
    }
    out.truncate(n);
    out
}

/// The timestamp a swept point or row is written at.
///
/// Fixed, so a query window is EXACTLY the window that was written. A query over a wide-open
/// range makes the server tile the time domain, which has already cost this project a 70-minute
/// hang in a different rollup path -- a benchmark must not be the thing that discovers that.
const BASE_TS_MS: u64 = 1_700_000_000_000;

/// Which fixture family an op belongs to. Carried rather than re-derived at each use, because
/// the four original arms and the four new ones differ in prefill, carriage check and arrival
/// check, and deciding that from a string in three separate places is how one of them drifts.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    /// `set` / `get` -- a single byte string under a key.
    StringLike,
    /// `hset` / `hget` -- one field of a hash.
    HashLike,
    /// `featappend` / `featquery` -- N points, each `value_bytes` wide.
    Feature,
    /// `seqadd` / `seqquery` -- N fixed-width rows; no width axis.
    Sequence,
}

/// Everything a worker thread needs to build one op. Cloned per thread; no shared mutable state,
/// so a concurrency sweep measures the SERVER rather than contention inside this harness.
#[derive(Clone)]
struct Fixture {
    family: Family,
    is_read: bool,
    key_bytes: usize,
    value_bytes: usize,
    points: usize,
    rows: usize,
    identical_payload: bool,
}

impl Fixture {
    fn key_at(&self, i: usize) -> String {
        format!("w{i:0width$}", width = self.key_bytes - 1)
    }

    /// The body of one point. `identical_payload` reproduces the historical fixture; the default
    /// is incompressible, keyed by the point's own identity so no two points share bytes.
    fn value_for(&self, i: usize, point: usize) -> Vec<u8> {
        if self.identical_payload {
            vec![b'v'; self.value_bytes]
        } else {
            incompressible((i as u64) << 20 | point as u64, self.value_bytes)
        }
    }

    fn feature_points(&self, i: usize) -> Vec<FeaturePoint> {
        (0..self.points)
            .map(|p| FeaturePoint {
                timestamp_ms: BASE_TS_MS + p as u64,
                value: self.value_for(i, p),
            })
            .collect()
    }

    /// A row's five scalars are all fixed-width, so the only thing a fixture can vary is how
    /// MANY rows an op carries. Values are derived from the indices so a row is distinguishable
    /// in a dump without being random (nothing here compresses, so entropy buys nothing).
    fn sequence_rows(&self, i: usize) -> Vec<SequenceFeatureRow> {
        (0..self.rows)
            .map(|r| SequenceFeatureRow {
                timestamp_ms: BASE_TS_MS + r as u64,
                gid: (i as u64) << 20 | r as u64,
                action_type: (r % 7) as u32,
                duration: (r % 997) as u32,
                author_id: (i as u64).wrapping_mul(2_654_435_761) ^ r as u64,
            })
            .collect()
    }

    /// The command that WRITES op `i`'s data. Used both for the timed write arms and, unchanged,
    /// for a read arm's untimed prefill -- so a read is always reading exactly what the matching
    /// write arm would have stored.
    fn write_command(&self, i: usize) -> Command {
        let key = self.key_at(i);
        match self.family {
            Family::StringLike => Command::StringSet { key, value: self.value_for(i, 0) },
            Family::HashLike => Command::HashSet {
                key,
                field: FIELD.to_string(),
                value: self.value_for(i, 0),
            },
            Family::Feature => Command::FeatureAppend { key, points: self.feature_points(i) },
            Family::Sequence => Command::SequenceAdd { key, rows: self.sequence_rows(i) },
        }
    }

    /// The command that READS op `i`'s data. The query window is exactly the written window.
    fn read_command(&self, i: usize) -> Command {
        let key = self.key_at(i);
        match self.family {
            Family::StringLike => Command::StringGet { key },
            Family::HashLike => Command::HashGet { key, field: FIELD.to_string() },
            Family::Feature => Command::FeatureQuery {
                key,
                start_ms: BASE_TS_MS,
                end_ms: BASE_TS_MS + self.points as u64,
                count: None,
            },
            Family::Sequence => Command::SequenceQuery {
                key,
                start_ms: BASE_TS_MS,
                end_ms: BASE_TS_MS + self.rows as u64,
                count: self.rows,
                filters: Vec::new(),
            },
        }
    }

    fn command_at(&self, i: usize) -> Command {
        if self.is_read {
            self.read_command(i)
        } else {
            self.write_command(i)
        }
    }
}

/// Did a response actually CARRY what was asked for?
///
/// The read-side version of the write arm's arrival check. An `ok` that carried nothing is a
/// miss, and a miss is cheaper than a read, so counting one as a success flatters the figure.
/// Every family needs its own answer because the carrier differs: a byte string, a point list,
/// a row list. A single `status.ok` test would pass for all of them while proving nothing.
fn response_carried(family: Family, response: &CommandResponse) -> bool {
    match family {
        Family::StringLike | Family::HashLike => {
            matches!(response, CommandResponse::Bytes { value: Some(_) })
        }
        Family::Feature => matches!(response, CommandResponse::FeaturePoints { points } if !points.is_empty()),
        Family::Sequence => matches!(response, CommandResponse::SequenceRows { rows } if !rows.is_empty()),
    }
}

/// The field name the hash arms use. One field, because the hash arms exist as a control for the
/// feature arms rather than as a hash sweep of their own.
const FIELD: &str = "f";

/// Name `EAGAIN` for what it is: THIS harness running out of local sockets, not the server
/// refusing load.
///
/// `post_json` opens a connection per op, so successive runs pile up `TIME_WAIT` sockets and the
/// next run cannot get an ephemeral port. Measured: a 1,200-op read run died at prefill 23 while
/// a 1,200-op write run of the same volume on the same server passed moments earlier. Reported
/// as a plain transport error this reads as a server limit and would be quoted as one.
fn describe_transport_failure(err: &str) -> String {
    // ANCHOR ON THE CLOSING PAREN. `"os error 11"` is a SUBSTRING of `"os error 111"`, which is
    // ECONNREFUSED -- so the unanchored form excused a genuine "server is not up" as socket
    // exhaustion. Caught by this function's own test, which is why that test pairs every
    // positive with an ordinary error that must NOT pick up the disclaimer.
    let exhausted = err.contains("(os error 11)")
        || err.contains("(os error 99)")
        || err.contains("Resource temporarily unavailable")
        || err.contains("Cannot assign requested address");
    if exhausted {
        format!(
            "transport: {err}\n\
             *** THIS IS THE HARNESS, NOT THE SERVER: local socket/ephemeral-port exhaustion \
             (one TCP connection per op, previous runs' sockets still in TIME_WAIT). Lower `ops`, \
             leave a few seconds between runs, and average over more REPEATS rather than more \
             ops. Figures from this run are not server figures."
        )
    } else {
        format!("transport: {err}")
    }
}

/// What one worker thread produced. `failed_first` is kept per thread and merged, so a failure
/// on thread 3 is not hidden by thread 0 succeeding.
struct ThreadOutcome {
    latencies_ms: Vec<f64>,
    ok: usize,
    failed_first: Option<String>,
}

/// Run `indices` sequentially on this thread, timing each round trip.
///
/// Split out so the concurrency sweep spawns N of these over DISJOINT index sets. Each op opens
/// its own request, so N threads give N in-flight requests, which is what "concurrency" means
/// to the server.
fn run_indices(
    addr: &str,
    fixture: &Fixture,
    indices: &[usize],
    sleep_ms: u64,
) -> ThreadOutcome {
    let mut out = ThreadOutcome {
        latencies_ms: Vec::with_capacity(indices.len()),
        ok: 0,
        failed_first: None,
    };
    for &i in indices {
        let request = ExecuteRequest { shard_id: 1, command: fixture.command_at(i) };
        let at = std::time::Instant::now();
        let outcome: Result<ExecuteResponse, _> = post_json(addr, "/execute", &request);
        let elapsed = at.elapsed().as_secs_f64() * 1000.0;
        match outcome {
            Ok(response) if response.status.ok => {
                let carried =
                    !fixture.is_read || response_carried(fixture.family, &response.response);
                if carried {
                    out.ok += 1;
                    out.latencies_ms.push(elapsed);
                } else if out.failed_first.is_none() {
                    out.failed_first = Some(format!(
                        "op {i} answered ok but carried no {} (a miss, not a read)",
                        match fixture.family {
                            Family::Feature => "points",
                            Family::Sequence => "rows",
                            _ => "value",
                        }
                    ));
                }
            }
            Ok(response) => {
                if out.failed_first.is_none() {
                    out.failed_first = Some(format!("status not ok: {:?}", response.status));
                }
            }
            Err(err) => {
                if out.failed_first.is_none() {
                    out.failed_first = Some(describe_transport_failure(&err.to_string()));
                }
            }
        }
        if sleep_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
        }
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let allow_debug = args.iter().any(|a| a == "--allow-debug");
    let positional: Vec<&String> = args[1..].iter().filter(|a| !a.starts_with("--")).collect();
    let addr = positional.first().map(|s| s.as_str()).unwrap_or_else(|| {
        eprintln!(
            "usage: <addr> [ops] [key_bytes] [value_bytes] [sleep_ms] [--allow-debug]\n\
             bring a server up with TS_META_ADDR=\"\" for standalone mode"
        );
        std::process::exit(2);
    });
    let ops: usize = positional.get(1).and_then(|s| s.parse().ok()).unwrap_or(200);
    let key_bytes: usize = positional.get(2).and_then(|s| s.parse().ok()).unwrap_or(16);
    let value_bytes: usize = positional.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let sleep_ms: u64 = positional.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
    // A FLAG RATHER THAN A POSITIONAL, so every invocation already in use keeps working and
    // keeps meaning `set`.
    let op = args
        .iter()
        .find_map(|a| a.strip_prefix("--op="))
        .unwrap_or("set")
        .to_string();
    let family = match op.as_str() {
        "set" | "get" => Family::StringLike,
        "hset" | "hget" => Family::HashLike,
        "featappend" | "featquery" => Family::Feature,
        "seqadd" | "seqquery" => Family::Sequence,
        other => {
            eprintln!(
                "--op must be set|get|hset|hget|featappend|featquery|seqadd|seqquery, got {other}"
            );
            std::process::exit(2);
        }
    };
    let is_read = matches!(op.as_str(), "get" | "hget" | "featquery" | "seqquery");
    let numeric_flag = |name: &str, default: usize| -> usize {
        args.iter()
            .find_map(|a| a.strip_prefix(name))
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let points = numeric_flag("--points=", 1);
    let rows = numeric_flag("--rows=", 1);
    let concurrency = numeric_flag("--concurrency=", 1).max(1);
    // DEFAULT DIFFERS BY ARM, DELIBERATELY. The four original arms keep `vec![b'v'; n]` so their
    // published figures stay comparable; the new arms default to incompressible because
    // `wal_compress_records` is on and an identical payload was 13.51x wrong at 1024 B.
    let payload = args.iter().find_map(|a| a.strip_prefix("--payload="));
    let identical_payload = match payload {
        Some("identical") => true,
        Some("random") => false,
        Some(other) => {
            eprintln!("--payload must be random|identical, got {other}");
            std::process::exit(2);
        }
        None => matches!(family, Family::StringLike | Family::HashLike),
    };
    assert!(points >= 1, "--points must be at least 1");
    assert!(rows >= 1, "--rows must be at least 1");
    // The engine's own ceiling is `feature_max_size` (10,000 by default) with a hard max above
    // it. Refuse here rather than letting the server reject mid-run and report the refusals as
    // latency -- a harness that measures its own rejections is the failure this file exists to
    // avoid on the read side.
    assert!(
        points <= 10_000 && rows <= 10_000,
        "--points/--rows above 10,000 exceed the engine's default feature_max_size; \
         the run would measure refusals"
    );

    // THE REFUSAL. Not a warning: a warning gets scrolled past and the number gets quoted.
    let debug_build = cfg!(debug_assertions);
    if debug_build && !allow_debug {
        eprintln!(
            "REFUSING: this harness was compiled WITHOUT optimisation, and a debug build of this \
             server measured 27.48 ms per write where release measured 4.74 ms -- a 5.8x swing \
             from the profile alone. A figure taken here would be about `-O0`, not about the \
             server.\n\
             Build and run with --release, or pass --allow-debug to get numbers stamped as not \
             being latency figures."
        );
        std::process::exit(3);
    }
    let stamp = if debug_build {
        "  [DEBUG - NOT A LATENCY FIGURE]"
    } else {
        ""
    };

    assert!(key_bytes >= 2, "key_bytes must leave room for a prefix and an index");
    let fixture = Fixture {
        family,
        is_read,
        key_bytes,
        value_bytes,
        points,
        rows,
        identical_payload,
    };
    assert_eq!(
        fixture.key_at(0).len(),
        key_bytes,
        "key width must be exactly as requested"
    );

    // PREFILL FOR A READ RUN, BEFORE THE CLOCK. Untimed, and its own read-back is what proves
    // there is anything to read -- a read run against an empty store would otherwise time misses
    // and report them as read latency.
    //
    // It goes through the fixture's OWN write command, so a read arm reads exactly what the
    // matching write arm would have stored. Building the prefill separately is how a read arm
    // ends up measuring a different shape than the write arm it is compared against.
    if is_read {
        for i in 0..ops {
            let seeded: Result<ExecuteResponse, _> = post_json(
                addr,
                "/execute",
                &ExecuteRequest { shard_id: 1, command: fixture.write_command(i) },
            );
            match seeded {
                Ok(r) if r.status.ok => {}
                Ok(r) => panic!("prefill {i} refused: {:?}", r.status),
                // The prefill is where socket exhaustion actually bit, so it needs the same
                // diagnosis as the timed loop -- otherwise the one place it reliably appears is
                // the one place that reports it as an ordinary transport error.
                Err(e) => {
                    panic!("prefill {i} {}", describe_transport_failure(&e.to_string()))
                }
            }
        }
    }
    let load_before = loadavg_1m();
    let io_before = io_pressure_some_avg10();
    let mem_before_kb = mem_available_kb();
    let d_state_before = d_state_tasks();

    // THE CONCURRENCY SWEEP'S ONE STRUCTURAL CHOICE: thread t takes every index where
    // `i % concurrency == t`, so the threads' key sets are DISJOINT. Pointing N threads at ONE
    // key would measure lock contention on that key, which is a different question and a much
    // more flattering one to answer by accident.
    let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); concurrency];
    for i in 0..ops {
        buckets[i % concurrency].push(i);
    }
    let fixture_ref = &fixture;
    let started = std::time::Instant::now();
    let outcomes: Vec<ThreadOutcome> = std::thread::scope(|scope| {
        let handles: Vec<_> = buckets
            .iter()
            .map(|indices| scope.spawn(move || run_indices(addr, fixture_ref, indices, sleep_ms)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("worker thread panicked"))
            .collect()
    });
    let wall = started.elapsed().as_secs_f64();
    let load_after = loadavg_1m();
    let io_after = io_pressure_some_avg10();
    let d_state_after = d_state_tasks();

    // Merge the per-thread outcomes. The percentiles are over the POOLED samples, which is what
    // a client sees; `failed_first` takes the first thread that had one so a failure on thread 3
    // is not swallowed by thread 0 succeeding.
    let mut latencies_ms: Vec<f64> = Vec::with_capacity(ops);
    let mut ok = 0usize;
    let mut failed_first: Option<String> = None;
    for outcome in outcomes {
        latencies_ms.extend(outcome.latencies_ms);
        ok += outcome.ok;
        if failed_first.is_none() {
            failed_first = outcome.failed_first;
        }
    }

    // ARRIVAL, PROVEN BY READ-BACK. A per-op status code says the server answered, not that it
    // stored anything -- so the last key is read back and COMPARED before any figure is treated
    // as being about writes. Each family compares its own carrier: a byte string, the point
    // list, the row list. `status.ok` alone is the shape that let a figure measure a no-op once.
    let last = ops.saturating_sub(1);
    let last_key = fixture.key_at(last);
    let read_back: Result<ExecuteResponse, _> = post_json(
        addr,
        "/execute",
        &ExecuteRequest { shard_id: 1, command: fixture.read_command(last) },
    );
    let arrived = match &read_back {
        Ok(r) if r.status.ok => match (&r.response, family) {
            (CommandResponse::Bytes { value: Some(v) }, Family::StringLike | Family::HashLike) => {
                v == &fixture.value_for(last, 0)
            }
            (CommandResponse::FeaturePoints { points: got }, Family::Feature) => {
                let want = fixture.feature_points(last);
                got.len() == want.len()
                    && got.last().map(|p| &p.value) == want.last().map(|p| &p.value)
            }
            (CommandResponse::SequenceRows { rows: got }, Family::Sequence) => {
                let want = fixture.sequence_rows(last);
                got.len() == want.len() && got.last() == want.last()
            }
            _ => false,
        },
        _ => false,
    };

    latencies_ms.sort_by(|a, b| a.partial_cmp(b).expect("no NaN latencies"));
    let mean = if latencies_ms.is_empty() {
        0.0
    } else {
        latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64
    };

    println!("addr             {addr}");
    println!("profile          {}{stamp}", if debug_build { "debug" } else { "release" });
    println!("op               {op}");
    println!("ops              {ops}");
    println!("ops_ok           {ok}");
    println!("key_bytes        {key_bytes}");
    // SAY WHEN AN AXIS DID NOTHING, rather than printing it as though it had. A sequence row is
    // five fixed-width scalars, so `value_bytes` cannot move a sequence figure -- printing it
    // bare would invite someone to read a row-count effect as a width effect.
    match family {
        Family::Sequence => println!(
            "value_bytes      {value_bytes}   (NOT APPLICABLE: a sequence row is five \
             fixed-width scalars)"
        ),
        Family::Feature => println!("value_bytes      {value_bytes}   (per point)"),
        _ => println!("value_bytes      {value_bytes}"),
    }
    match family {
        Family::Feature => println!("points_per_op    {points}"),
        Family::Sequence => println!("rows_per_op      {rows}"),
        _ => {}
    }
    println!(
        "payload          {}",
        if identical_payload {
            "identical   (all-identical bytes: a COMPRESSIBILITY CONTROL, not a workload)"
        } else {
            "incompressible   (seeded xorshift, distinct per point)"
        }
    );
    println!("concurrency      {concurrency}   (threads over disjoint key sets)");
    println!("sleep_ms_per_op  {sleep_ms}   (EXCLUDED from the per-op figures below)");
    println!(
        "arrival_verified {arrived}   (read-back of {last_key} compared{}{})",
        match family {
            Family::Feature => ", point count and last value",
            Family::Sequence => ", row count and last row",
            _ => " byte for byte",
        },
        if is_read { ", prefilled before the clock" } else { "" }
    );
    // THE REAL CONTENTION SIGNALS. See the module note: loadavg is retained for continuity and
    // must not be used to judge whether a run is comparable.
    println!("io_pressure_some_avg10  before={io_before} after={io_after}");
    println!("mem_available_kb        {mem_before_kb}");
    println!("d_state_tasks           before={d_state_before} after={d_state_after}");
    println!("loadavg_before   {load_before}{stamp}   (NOT a contention signal here)");
    println!("loadavg_after    {load_after}{stamp}   (NOT a contention signal here)");
    if latencies_ms.is_empty() {
        println!("per_op_ms        none -- every op failed");
        if let Some(why) = failed_first {
            println!("first_failure    {why}");
        }
        std::process::exit(1);
    }
    println!("per_op_min_ms    {:.3}{stamp}", latencies_ms[0]);
    println!("per_op_p50_ms    {:.3}{stamp}", percentile_sorted(&latencies_ms, 50));
    println!("per_op_p90_ms    {:.3}{stamp}", percentile_sorted(&latencies_ms, 90));
    println!("per_op_p99_ms    {:.3}{stamp}", percentile_sorted(&latencies_ms, 99));
    println!("per_op_max_ms    {:.3}{stamp}", latencies_ms[latencies_ms.len() - 1]);
    println!("per_op_mean_ms   {:.3}{stamp}", mean);
    println!("wall_s           {wall:.3}   (INCLUDES the sleeps)");
    // THROUGHPUT BESIDE THE PERCENTILES. A p50 that improves while ops/s falls is a worse
    // server, and the percentiles alone will not say so -- which is exactly what a concurrency
    // sweep is for. Computed over SUCCESSFUL ops so a run that half failed cannot look fast.
    if wall > 0.0 {
        println!("ops_per_s        {:.1}{stamp}   (ops_ok / wall)", ok as f64 / wall);
    }
    if let Some(why) = failed_first {
        println!("first_failure    {why}");
    }
    if !arrived {
        println!(
            "WARNING: the read-back did not succeed, so these figures are not demonstrably \
             about stored writes"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{describe_transport_failure, incompressible, response_carried, Family, Fixture};
    use super::{percentile_sorted, BASE_TS_MS};
    use temporalstore_rust::types::{Command, CommandResponse, FeaturePoint};

    fn fixture(family: Family, is_read: bool, points: usize, rows: usize) -> Fixture {
        Fixture {
            family,
            is_read,
            key_bytes: 16,
            value_bytes: 32,
            points,
            rows,
            identical_payload: false,
        }
    }

    /// THE FIXTURE ERROR THIS GUARDS: `vec![b'v'; n]` compresses to nothing, and
    /// `wal_compress_records` is on by default, so an identical payload measured the compressor
    /// and was 13.51x wrong at 1024 B. A generator that returned a constant would pass a length
    /// check and reintroduce exactly that, so this asserts the bytes VARY -- and that two
    /// different seeds disagree, which a single-seed check would not catch.
    #[test]
    fn the_default_payload_is_not_a_constant_and_differs_by_seed() {
        let a = incompressible(1, 256);
        assert_eq!(a.len(), 256, "requested width must be exact");
        assert!(
            a.windows(2).any(|w| w[0] != w[1]),
            "an all-identical body is the compressibility bug this exists to avoid"
        );
        let distinct = a.iter().collect::<std::collections::BTreeSet<_>>().len();
        assert!(distinct > 32, "only {distinct} distinct byte values in 256 -- too regular");
        assert_ne!(incompressible(1, 256), incompressible(2, 256), "seeds must disagree");
    }

    /// Reproducible across calls, because a latency cell is run three times and the three runs
    /// must differ only in TIMING. A payload that changed per call would put fixture variation
    /// into the between-run spread, which is the number used to decide whether an effect is real.
    #[test]
    fn the_payload_is_reproducible_so_repeat_runs_differ_only_in_timing() {
        assert_eq!(incompressible(7, 64), incompressible(7, 64));
        assert_eq!(incompressible(0, 1).len(), 1, "a width under one word must still be exact");
        assert!(incompressible(3, 0).is_empty());
    }

    /// The two new axes must be INDEPENDENT: point count and point width. If `--points` silently
    /// scaled width (or vice versa) the sweep could not separate a field-count effect from a
    /// byte effect, which is the whole reason the sweep exists.
    #[test]
    fn point_count_and_point_width_are_separate_axes() {
        let narrow_many = fixture(Family::Feature, false, 16, 1);
        let points = narrow_many.feature_points(0);
        assert_eq!(points.len(), 16, "point count must follow --points");
        assert!(points.iter().all(|p| p.value.len() == 32), "width must follow value_bytes");
        // Same total bytes, opposite split -- the comparison the field-count model needs.
        let wide_few = Fixture { points: 1, value_bytes: 512, ..narrow_many.clone() };
        assert_eq!(wide_few.feature_points(0).len(), 1);
        assert_eq!(wide_few.feature_points(0)[0].value.len(), 512);
        // Distinct bodies per point, so no two points in one append share bytes.
        assert_ne!(points[0].value, points[1].value);
    }

    /// A sequence row has NO width axis -- five fixed-width scalars. This pins that `value_bytes`
    /// cannot move a sequence fixture, so the output's "NOT APPLICABLE" note stays true.
    #[test]
    fn a_sequence_row_has_no_width_axis() {
        let narrow = fixture(Family::Sequence, false, 1, 8);
        let wide = Fixture { value_bytes: 4096, ..narrow.clone() };
        assert_eq!(narrow.sequence_rows(3), wide.sequence_rows(3), "width must not reach a row");
        assert_eq!(narrow.sequence_rows(3).len(), 8, "row count must follow --rows");
    }

    /// The query window must be EXACTLY the written window. A wide-open range makes the server
    /// tile the time domain, which cost this project a 70-minute hang in a different rollup path.
    #[test]
    fn a_read_queries_exactly_the_window_that_was_written() {
        let f = fixture(Family::Feature, true, 16, 1);
        let written = f.feature_points(0);
        let first = written.first().expect("points").timestamp_ms;
        let last = written.last().expect("points").timestamp_ms;
        match f.read_command(0) {
            Command::FeatureQuery { start_ms, end_ms, .. } => {
                assert!(start_ms <= first && end_ms >= last, "window must cover what was written");
                assert_eq!(start_ms, BASE_TS_MS, "an open-ended start walks the time domain");
                assert!(end_ms - start_ms <= 16, "the window must be tight, not the whole domain");
            }
            other => panic!("feature read must be a FeatureQuery, got {other:?}"),
        }
    }

    /// A MISS IS NOT A READ, and each family has a different carrier. An empty point list or an
    /// empty row list is the feature-side equivalent of `Bytes { value: None }` -- cheaper than
    /// a real read, so counting it as a success flatters the figure.
    #[test]
    fn an_empty_carrier_is_a_miss_for_every_family() {
        assert!(!response_carried(Family::Feature, &CommandResponse::FeaturePoints {
            points: Vec::new()
        }));
        assert!(response_carried(Family::Feature, &CommandResponse::FeaturePoints {
            points: vec![FeaturePoint { timestamp_ms: BASE_TS_MS, value: vec![1] }]
        }));
        assert!(!response_carried(Family::Sequence, &CommandResponse::SequenceRows {
            rows: Vec::new()
        }));
        assert!(!response_carried(Family::StringLike, &CommandResponse::Bytes { value: None }));
        // A response of the WRONG SHAPE is also a miss, not a pass: a feature arm that received
        // `Bytes` has not read points, however `ok` the status was.
        assert!(!response_carried(Family::Feature, &CommandResponse::Bytes { value: Some(vec![1]) }));
    }

    /// `EAGAIN` must be named as the HARNESS's limit, because it reads exactly like a server
    /// refusing load and was in fact local socket exhaustion -- a 1,200-op read run died at
    /// prefill 23 while a 1,200-op write run of the same volume passed moments earlier. An
    /// ordinary transport error must NOT pick up the disclaimer, or the disclaimer means nothing.
    #[test]
    fn socket_exhaustion_is_reported_as_the_harness_not_the_server() {
        for exhausted in [
            "io error: Resource temporarily unavailable (os error 11)",
            "io error: Cannot assign requested address (os error 99)",
        ] {
            let msg = describe_transport_failure(exhausted);
            assert!(msg.contains("THIS IS THE HARNESS, NOT THE SERVER"), "missing for {exhausted}");
            assert!(msg.contains(exhausted), "the original error must survive");
        }
        let ordinary = describe_transport_failure("io error: connection refused (os error 111)");
        assert!(
            !ordinary.contains("THIS IS THE HARNESS"),
            "a genuine connection failure must not be excused as socket exhaustion"
        );
        assert!(ordinary.contains("connection refused"));
    }

    /// A read arm must read what the matching write arm stored. The prefill goes through
    /// `write_command`, so this pins that the two agree on the key for the same index -- the
    /// failure would be a read arm quietly measuring a different shape than its write arm.
    #[test]
    fn a_read_arm_and_its_write_arm_address_the_same_key() {
        for family in [Family::StringLike, Family::HashLike, Family::Feature, Family::Sequence] {
            let w = fixture(family, false, 4, 4);
            let r = fixture(family, true, 4, 4);
            assert_eq!(w.key_at(9), r.key_at(9), "write and read must share the key");
            assert_eq!(w.key_at(9).len(), 16, "key width must be exact");
        }
    }

    /// Nearest-rank, so every answer is a sample that occurred. With ten distinct samples the
    /// p50 is the fifth and the p90 the ninth -- not an average of neighbours.
    #[test]
    fn a_percentile_is_a_sample_that_actually_occurred() {
        let sorted: Vec<f64> = (1..=10).map(|n| n as f64).collect();
        assert_eq!(percentile_sorted(&sorted, 50), 5.0);
        assert_eq!(percentile_sorted(&sorted, 90), 9.0);
        assert_eq!(percentile_sorted(&sorted, 99), 10.0);
        assert_eq!(percentile_sorted(&sorted, 100), 10.0);
    }

    /// THE OFF-BY-ONE THIS GUARD EXISTS FOR: `(len * pct) / 100` indexes one past the end at
    /// pct = 100, which panics on a slice. A harness that panics after the run has thrown the
    /// measurement away, so the clamp is load-bearing rather than defensive.
    #[test]
    fn the_top_percentile_cannot_index_past_the_end() {
        for len in 1..=200usize {
            let sorted: Vec<f64> = (0..len).map(|n| n as f64).collect();
            for pct in [1usize, 50, 90, 99, 100] {
                let got = percentile_sorted(&sorted, pct);
                assert!(
                    got <= (len - 1) as f64,
                    "len {len} pct {pct} produced {got}, which is outside the samples"
                );
            }
        }
    }

    /// An empty run must report zero rather than panicking, because "every op failed" is a real
    /// outcome of pointing this at a server that is not up and the harness has to survive it to
    /// say so.
    #[test]
    fn an_empty_sample_set_does_not_panic() {
        assert_eq!(percentile_sorted(&[], 50), 0.0);
        assert_eq!(percentile_sorted(&[], 100), 0.0);
    }
}
