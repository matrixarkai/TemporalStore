// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//
// Large-file / attachment throughput bench for the shared-store blob path.
// Measures append_blob (chunked write) MB/s + whole-object read MB/s for a
// range of attachment sizes against the FileObjectStore backend, and against
// the in-process MatrixObject store when built with `--features matrixobject`.
//
// Usage:
//   large_blob_throughput_bench [--backend file|matrixobject] \
//       [--sizes-mb 1,16,128] [--chunk-kb 1024] [--root <dir>]

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore};

struct Opts {
    backend: String,
    sizes_mb: Vec<usize>,
    chunk_kb: usize,
    root: Option<String>,
}

fn parse_opts() -> Opts {
    let mut o = Opts {
        backend: "file".to_string(),
        sizes_mb: vec![1, 16, 128],
        chunk_kb: 1024,
        root: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(k) = args.next() {
        let v = args.next().unwrap_or_default();
        match k.as_str() {
            "--backend" => o.backend = v,
            "--sizes-mb" => {
                o.sizes_mb = v.split(',').map(|s| s.trim().parse().unwrap()).collect()
            }
            "--chunk-kb" => o.chunk_kb = v.parse().unwrap(),
            "--root" => o.root = Some(v),
            _ => {}
        }
    }
    o
}

async fn bench_store<O: ObjectStore>(store: Arc<O>, backend: &str, sizes_mb: &[usize], chunk_kb: usize) {
    let chunk_bytes = chunk_kb * 1024;
    let chunk = Bytes::from(vec![0x5au8; chunk_bytes]);
    println!("[");
    let mut first = true;
    for &size_mb in sizes_mb {
        let total_bytes = size_mb * 1024 * 1024;
        let n_chunks = total_bytes / chunk_bytes;
        let key = format!("bench/attach-{size_mb}mb.blob");
        // best-effort clean
        let _ = store.delete(&key).await;

        // WRITE: chunked append_blob
        let w_start = Instant::now();
        for _ in 0..n_chunks {
            store.append_blob(&key, chunk.clone()).await.expect("append_blob");
        }
        let w_elapsed = w_start.elapsed();
        let written = (n_chunks * chunk_bytes) as f64;
        let w_mbps = (written / (1024.0 * 1024.0)) / w_elapsed.as_secs_f64();
        let w_p_chunk_ms = w_elapsed.as_secs_f64() * 1000.0 / n_chunks as f64;

        // READ: whole-object get
        let r_start = Instant::now();
        let got = store.get(&key).await.expect("get");
        let r_elapsed = r_start.elapsed();
        let r_mbps = (got.len() as f64 / (1024.0 * 1024.0)) / r_elapsed.as_secs_f64();

        let _ = store.delete(&key).await;
        if !first {
            println!(",");
        }
        first = false;
        print!(
            "  {{\"backend\":\"{backend}\",\"size_mb\":{size_mb},\"chunk_kb\":{chunk_kb},\"n_chunks\":{n_chunks},\
\"write_mb_s\":{w_mbps:.1},\"write_ms_per_chunk\":{w_p_chunk_ms:.3},\"write_total_ms\":{:.1},\
\"read_mb_s\":{r_mbps:.1},\"read_total_ms\":{:.1}}}",
            w_elapsed.as_secs_f64() * 1000.0,
            r_elapsed.as_secs_f64() * 1000.0
        );
    }
    println!("\n]");
}

#[tokio::main]
async fn main() {
    let o = parse_opts();
    let root = o
        .root
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("ts-blob-bench").display().to_string());

    match o.backend.as_str() {
        "file" => {
            let store = Arc::new(FileObjectStore::new(std::path::PathBuf::from(&root)));
            bench_store(store, "file", &o.sizes_mb, o.chunk_kb).await;
        }
        #[cfg(feature = "matrixobject")]
        "matrixobject" => {
            use matrixobjectstore_rs::StoreOptions;
            use temporalstore_rust::MatrixObjectObjectStore;
            let store = Arc::new(
                MatrixObjectObjectStore::new("blob-bench", StoreOptions::default()).unwrap(),
            );
            bench_store(store, "matrixobject", &o.sizes_mb, o.chunk_kb).await;
        }
        other => {
            eprintln!("unsupported backend {other:?} (built without matrixobject feature?)");
            std::process::exit(2);
        }
    }
}
