// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//
// End-to-end large-file / attachment throughput bench THROUGH the datanode HTTP
// path. Uploads bodies of several sizes to `POST /blob/<key>` (which streams to
// the blob store via ObjectStore::append_blob) and reads them back from
// `GET /blob/<key>`, reporting write/read MB/s and latency. Point it at a running
// datanode that serves the blob endpoints (see the accompanying run script).
//
// Usage:
//   blob_http_bench --addr 127.0.0.1:17190 --sizes-mb 1,16,128 --reps 2

use std::time::Instant;

use temporalstore_rust::http::{request_bytes_with_options, HttpRequestOptions};

struct Opts {
    addr: String,
    sizes_mb: Vec<usize>,
    reps: usize,
    io_timeout_ms: u64,
}

fn parse_opts() -> Opts {
    let mut o = Opts {
        addr: "127.0.0.1:17190".to_string(),
        sizes_mb: vec![1, 16, 128],
        reps: 2,
        io_timeout_ms: 120_000,
    };
    let mut args = std::env::args().skip(1);
    while let Some(k) = args.next() {
        let v = args.next().unwrap_or_default();
        match k.as_str() {
            "--addr" => o.addr = v,
            "--sizes-mb" => {
                o.sizes_mb = v.split(',').map(|s| s.trim().parse().unwrap()).collect()
            }
            "--reps" => o.reps = v.parse().unwrap(),
            "--io-timeout-ms" => o.io_timeout_ms = v.parse().unwrap(),
            _ => {}
        }
    }
    o
}

fn main() {
    let o = parse_opts();
    let options = HttpRequestOptions {
        connect_timeout_ms: 2_000,
        io_timeout_ms: o.io_timeout_ms,
        max_retries: 0,
    };
    println!("[");
    let mut first = true;
    for &size_mb in &o.sizes_mb {
        let total = size_mb * 1024 * 1024;
        let payload = vec![0x5au8; total];
        let key = format!("attach-{size_mb}mb");
        let path = format!("/blob/{key}");
        let mut w_best = f64::MAX;
        let mut r_best = f64::MAX;
        let mut ok = true;
        for _ in 0..o.reps.max(1) {
            // WRITE (POST): stream body to the blob store through HTTP.
            let w0 = Instant::now();
            let resp =
                request_bytes_with_options(&o.addr, "POST", &path, &payload, "application/octet-stream", options);
            let w_ms = w0.elapsed().as_secs_f64() * 1000.0;
            if resp.is_err() {
                eprintln!("POST {path} failed: {:?}", resp.err());
                ok = false;
                break;
            }
            w_best = w_best.min(w_ms);

            // READ (GET): stream body back.
            let r0 = Instant::now();
            let got = request_bytes_with_options(&o.addr, "GET", &path, &[], "application/octet-stream", options);
            let r_ms = r0.elapsed().as_secs_f64() * 1000.0;
            match got {
                Ok(bytes) if bytes.len() == total => r_best = r_best.min(r_ms),
                Ok(bytes) => {
                    eprintln!("GET {path} length mismatch: {} != {total}", bytes.len());
                    ok = false;
                    break;
                }
                Err(err) => {
                    eprintln!("GET {path} failed: {err:?}");
                    ok = false;
                    break;
                }
            }
        }
        if !first {
            println!(",");
        }
        first = false;
        if ok {
            let mb = total as f64 / (1024.0 * 1024.0);
            print!(
                "  {{\"size_mb\":{size_mb},\"write_mb_s\":{:.1},\"write_ms\":{:.1},\"read_mb_s\":{:.1},\"read_ms\":{:.1}}}",
                mb / (w_best / 1000.0),
                w_best,
                mb / (r_best / 1000.0),
                r_best
            );
        } else {
            print!("  {{\"size_mb\":{size_mb},\"error\":true}}");
        }
    }
    println!("\n]");
}
