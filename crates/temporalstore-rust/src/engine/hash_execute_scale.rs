// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::sdk::v1::LlmMemoryRecord;
use prost::Message;

fn cpu_ns() -> u64 {
    std::fs::read_to_string("/proc/thread-self/schedstat")
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

fn record(index: usize) -> LlmMemoryRecord {
    let id = format!("{:016x}-{index:016x}", 42);
    LlmMemoryRecord {
        id: id.clone(),
        context_ref: id,
        memory: if index < 64 {
            format!("oldest original {index}: {}", "payload word ".repeat(2048))
        } else {
            format!("background original {index:08}")
        },
        user_id: "user".into(),
        agent_id: "agent".into(),
        run_id: "run".into(),
        updated_at_ms: 1,
        ..Default::default()
    }
}

#[test]
#[ignore = "manual Linux backend scale experiment"]
fn single_hash_write_scale() {
    let api = std::env::var("HASH_EXECUTE_API").unwrap_or_else(|_| "execute".into());
    assert!(matches!(api.as_str(), "execute" | "batch_execute"));
    let count: usize = std::env::var("HASH_EXECUTE_ROWS")
        .unwrap_or_else(|_| "8192".into())
        .parse()
        .unwrap();
    assert!((8192..=262144).contains(&count) && count % 64 == 0);
    let parent = std::env::var_os("TEMPORALSTORE_BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = tempfile::Builder::new()
        .prefix("hash-filing-scale-")
        .tempdir_in(parent)
        .unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut timings = Vec::with_capacity(count / 64);
    let mut payload_bytes = 0usize;
    let filed_before = storage_bucket_internals::bucket_block_upserts();
    let cpu_before = cpu_ns();
    let started = std::time::Instant::now();
    for start in (0..count).step_by(64) {
        let entries = (start..start + 64)
            .map(|index| {
                let record = record(index);
                let bytes = record.encode_to_vec();
                payload_bytes += bytes.len();
                (record.id, bytes)
            })
            .collect();
        let before = std::time::Instant::now();
        let command = Command::HashMultiSet {
            key: "row".into(),
            entries,
        };
        if api == "execute" {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "{:?}", response.status);
        } else {
            let response = engine.batch_execute(BatchExecuteRequest {
                shard_id: 1,
                commands: vec![command],
            });
            assert!(response.status.ok, "{:?}", response.status);
            assert_eq!(response.responses.len(), 1);
            assert!(
                response.responses[0].status.ok,
                "{:?}",
                response.responses[0].status
            );
        }
        timings.push(before.elapsed().as_nanos() as u64);
        let end = start + 64;
        if end.is_power_of_two() || end == count {
            eprintln!(
                "rows={end} elapsed_ms={} last_batch_ns={}",
                started.elapsed().as_millis(),
                timings.last().unwrap()
            );
        }
    }
    let elapsed_ms = started.elapsed().as_millis();
    let cpu_elapsed = cpu_ns() - cpu_before;
    let refiled = storage_bucket_internals::bucket_block_upserts() - filed_before;
    assert_eq!(refiled, 0);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashLen { key: "row".into() },
    });
    assert!(response.status.ok);
    assert!(
        matches!(response.response, CommandResponse::Integer { value } if value == count as i64)
    );
    let requested = (0..64).chain(count - 64..count).collect::<Vec<_>>();
    let ids = requested.iter().map(|index| record(*index).id).collect();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiGet {
            key: "row".into(),
            fields: ids,
        },
    });
    assert!(response.status.ok);
    let CommandResponse::Values { values } = response.response else {
        panic!("hash values expected")
    };
    assert_eq!(values.len(), requested.len());
    for (bytes, index) in values.into_iter().zip(requested) {
        assert_eq!(
            LlmMemoryRecord::decode(bytes.unwrap().as_slice()).unwrap(),
            record(index)
        );
    }
    let mut tail = timings[timings.len() - 64..].to_vec();
    tail.sort_unstable();
    let peak_rss = std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:").map(|value| {
                value
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
        })
        .unwrap();
    println!(
        "HASH_EXECUTE_SCALE {}",
        serde_json::json!({
            "corpus_rows": count, "payload_bytes": payload_bytes, "batch_rows": 64,
            "elapsed_ms": elapsed_ms, "worker_thread_cpu_ns": cpu_elapsed,
            "tail_batches": tail.len(), "tail_p50_ns": tail[31], "tail_p95_ns": tail[60], "tail_p99_ns": tail[63],
            "peak_whole_process_rss_kib": peak_rss, "post_write_refilings": refiled,
            "verified_oldest_and_newest_payloads": 128, "ingestion_api": api,
            "scope": "backend unit-test process; excludes network, models and distributed fan-out",
            "all_workflows_flat": false,
        })
    );
}
