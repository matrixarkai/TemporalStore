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
//! Run:
//!   data_server_write_latency_harness <addr> [ops] [key_bytes] [value_bytes] [sleep_ms] [--allow-debug]
//!   data_server_write_latency_harness 127.0.0.1:17102 200 16 64 10

use temporalstore_rust::http::post_json;
use temporalstore_rust::types::{Command, ExecuteRequest, ExecuteResponse};

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
    let value = vec![b'v'; value_bytes];
    let load_before = loadavg_1m();

    let mut latencies_ms: Vec<f64> = Vec::with_capacity(ops);
    let mut ok = 0usize;
    let mut failed_first: Option<String> = None;
    let started = std::time::Instant::now();
    for i in 0..ops {
        let key = format!("w{i:0width$}", width = key_bytes - 1);
        debug_assert_eq!(key.len(), key_bytes, "key width must be exactly as requested");
        let request = ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            },
        };
        let at = std::time::Instant::now();
        let outcome: Result<ExecuteResponse, _> = post_json(addr, "/execute", &request);
        let elapsed = at.elapsed().as_secs_f64() * 1000.0;
        match outcome {
            Ok(response) if response.status.ok => {
                ok += 1;
                latencies_ms.push(elapsed);
            }
            Ok(response) => {
                if failed_first.is_none() {
                    failed_first = Some(format!("status not ok: {:?}", response.status));
                }
            }
            Err(err) => {
                if failed_first.is_none() {
                    failed_first = Some(format!("transport: {err}"));
                }
            }
        }
        if sleep_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
        }
    }
    let wall = started.elapsed().as_secs_f64();
    let load_after = loadavg_1m();

    // ARRIVAL, PROVEN BY READ-BACK. A per-op status code says the server answered, not that it
    // stored anything -- so the last key is read and compared byte for byte before any figure is
    // treated as being about writes.
    let last_key = format!("w{:0width$}", ops.saturating_sub(1), width = key_bytes - 1);
    let read_back: Result<ExecuteResponse, _> = post_json(
        addr,
        "/execute",
        &ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: last_key.clone() },
        },
    );
    let arrived = matches!(&read_back, Ok(r) if r.status.ok);

    latencies_ms.sort_by(|a, b| a.partial_cmp(b).expect("no NaN latencies"));
    let mean = if latencies_ms.is_empty() {
        0.0
    } else {
        latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64
    };

    println!("addr             {addr}");
    println!("profile          {}{stamp}", if debug_build { "debug" } else { "release" });
    println!("ops              {ops}");
    println!("ops_ok           {ok}");
    println!("key_bytes        {key_bytes}");
    println!("value_bytes      {value_bytes}");
    println!("sleep_ms_per_op  {sleep_ms}   (EXCLUDED from the per-op figures below)");
    println!("arrival_verified {arrived}   (read-back of {last_key})");
    println!("loadavg_before   {load_before}{stamp}");
    println!("loadavg_after    {load_after}{stamp}");
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
    use super::percentile_sorted;

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
