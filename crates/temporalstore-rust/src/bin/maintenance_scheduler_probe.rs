// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Run bounded scheduled maintenance, then verify the same directory in a new process.
//! Usage: maintenance_scheduler_probe <seed-run|verify> <directory> <string|hash>
//! <keys> <rounds> <overwrite_every> <routing_end> <memory_bytes>

use std::path::PathBuf;
use std::time::{Duration, Instant};
use temporalstore_rust::control::{Config, LoadShardRequest, SetConfigRequest};
use temporalstore_rust::data_node::{DataNodeRuntime, DataNodeRuntimeOptions, StorageManagerOptions};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::types::{Command, CommandResponse, ExecuteRequest};

const SHARD: u64 = 1;

#[derive(Debug)]
struct Args {
    verify: bool,
    directory: PathBuf,
    hash: bool,
    keys: usize,
    rounds: u64,
    overwrite_every: usize,
    routing_end: u32,
    memory_bytes: usize,
}

fn arguments(args: &[String]) -> Args {
    assert_eq!(args.len(), 9, "expected phase, directory, model, keys, rounds, overwrite_every, routing_end, memory_bytes");
    assert!(matches!(args[1].as_str(), "seed-run" | "verify"));
    assert!(matches!(args[3].as_str(), "string" | "hash"));
    let result = Args {
        verify: args[1] == "verify",
        directory: PathBuf::from(&args[2]),
        hash: args[3] == "hash",
        keys: args[4].parse().expect("keys must be an integer"),
        rounds: args[5].parse().expect("rounds must be an integer"),
        overwrite_every: args[6].parse().expect("overwrite_every must be an integer"),
        routing_end: args[7].parse().expect("routing_end must be an integer"),
        memory_bytes: args[8].parse().expect("memory_bytes must be an integer"),
    };
    assert!((1..=1_000_000).contains(&result.keys));
    assert!((1..=64).contains(&result.rounds));
    assert!(result.memory_bytes >= 1 << 20);
    result
}

fn key(index: usize) -> String {
    let mut key = format!("k{index}");
    let alphabet = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut state = index.wrapping_mul(2_654_435_761);
    while key.len() < 16 {
        state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        key.push(alphabet[(state >> 16) % alphabet.len()] as char);
    }
    key
}

fn value(index: usize, overwritten: bool) -> Vec<u8> {
    let mut value = Vec::with_capacity(64);
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    while value.len() < 64 {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut bits = state;
        bits = (bits ^ (bits >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        bits = (bits ^ (bits >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        bits ^= bits >> 31;
        value.extend_from_slice(&bits.to_le_bytes());
    }
    value[..8].copy_from_slice(&(index as u64).to_le_bytes());
    value[8..16].copy_from_slice(&u64::from(overwritten).to_le_bytes());
    value
}

fn overwritten(args: &Args, index: usize) -> bool {
    args.overwrite_every != 0 && index % args.overwrite_every == 0
}

fn write(engine: &TemporalEngine, args: &Args, index: usize, overwrite: bool) {
    let command = if args.hash {
        Command::HashSet { key: key(index), field: "f".into(), value: value(index, overwrite) }
    } else {
        Command::StringSet { key: key(index), value: value(index, overwrite) }
    };
    let result = engine.execute(ExecuteRequest { shard_id: SHARD, command });
    assert!(result.status.ok, "write {index}: {:?}", result.status);
}

fn snapshot(engine: &TemporalEngine) -> serde_json::Value {
    let summaries = engine.bucket_storage_summaries(SHARD);
    serde_json::json!({
        "dirty_buckets": summaries.iter().filter(|row| row.dirty_object_count > 0).count(),
        "objects": summaries.iter().map(|row| row.object_count).sum::<u64>(),
        "wal": engine.write_ahead_log_store().stats(SHARD),
        "active_slabs": engine.block_store().slab_ids().expect("active slab inventory"),
        "quarantined_slabs": engine.block_store().delayed_destroy_slab_ids().expect("quarantine inventory"),
        "manifests": engine.list_bucket_dump_manifests(SHARD).len(),
    })
}

fn main() {
    let args = arguments(&std::env::args().collect::<Vec<_>>());
    if args.verify {
        assert!(args.directory.is_dir(), "verification directory is missing");
    } else {
        std::fs::create_dir_all(&args.directory).expect("create probe directory");
        assert!(std::fs::read_dir(&args.directory).unwrap().next().is_none(),
            "seed-run refuses a nonempty directory");
    }
    let open_started = Instant::now();
    let engine = TemporalEngine::with_local_dirs(
        args.memory_bytes, args.directory.join("cache"), args.directory.join("blocks"),
        args.directory.join("indexes"),
    );
    let load_started = Instant::now();
    let loaded = engine.load_shard_with(LoadShardRequest {
        shard_id: SHARD, load_version: 0, local_node_id: None, shard_uri: String::new(),
        start_routing_bucket: 0, end_routing_bucket: args.routing_end,
        readonly: false, table_name: String::new(),
    });
    assert!(loaded.status.ok, "load failed: {:?}", loaded.status);
    let load_us = load_started.elapsed().as_micros();
    let open_load_us = open_started.elapsed().as_micros();
    if args.verify {
        engine.cache().invalidate_shard(SHARD).expect("invalidate verification cache");
        for index in 0..args.keys {
            let command = if args.hash {
                Command::HashGet { key: key(index), field: "f".into() }
            } else {
                Command::StringGet { key: key(index) }
            };
            let result = engine.execute(ExecuteRequest { shard_id: SHARD, command });
            assert!(result.status.ok, "read {index}: {:?}", result.status);
            assert_eq!(result.response,
                CommandResponse::Bytes { value: Some(value(index, overwritten(&args, index))) },
                "restored value {index}");
        }
        println!("SCHEDULER_RESTORE {}", serde_json::json!({
            "keys": args.keys, "verified_exact": args.keys, "load_us": load_us,
            "open_load_us": open_load_us, "fresh_process": true, "cold_filesystem_cache": false,
        }));
        return;
    }
    assert!(engine.set_config(SetConfigRequest {
        shard_id: SHARD, config: Config { version: 2, async_storage: true, ..Default::default() },
    }).ok);
    let seed_started = Instant::now();
    for index in 0..args.keys {
        write(&engine, &args, index, false);
    }
    let mut overwrites = 0;
    for index in 0..args.keys {
        if overwritten(&args, index) {
            write(&engine, &args, index, true);
            overwrites += 1;
        }
    }
    engine.write_ahead_log_store().flush(SHARD).expect("flush seeded log");
    let seed_us = seed_started.elapsed().as_micros();
    let before = snapshot(&engine);
    let runtime = DataNodeRuntime::new(engine.clone(), DataNodeRuntimeOptions {
        worker_threads: 1, max_queue_depth: 4, max_background_queue_depth: 2,
    });
    let initial = runtime.stats();
    assert_eq!(initial.storage_manager_loops, 0);
    let scheduler = runtime.start_storage_manager_scheduler_for_all_shards(
        Duration::from_millis(250), StorageManagerOptions::default(),
    );
    let scheduled_started = Instant::now();
    loop {
        let completed = runtime.stats().storage_manager_loops;
        assert!(completed <= args.rounds, "observer missed the requested round count");
        if completed == args.rounds { break; }
        assert!(scheduled_started.elapsed() < Duration::from_secs(600), "scheduler did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    scheduler.stop();
    let stats = runtime.stats();
    assert_eq!(stats.storage_manager_loops, args.rounds, "extra round after stopping");
    assert_eq!(stats.storage_manager_failed_rounds, 0, "maintenance failed");
    assert!(!stats.storage_manager_stage_timing.is_empty());
    let after = snapshot(&engine);
    engine.write_ahead_log_store().flush(SHARD).expect("flush final log");
    engine.block_store().sync_durable().expect("sync final blocks");
    println!("SCHEDULER_COST {}", serde_json::json!({
        "keys": args.keys, "overwrites": overwrites, "rounds": args.rounds,
        "routing_end": args.routing_end, "memory_bytes": args.memory_bytes,
        "seed_us": seed_us, "scheduler_wall_us_including_intervals": scheduled_started.elapsed().as_micros(),
        "before": before, "after": after,
        "stage_timing": stats.storage_manager_stage_timing,
        "failed_rounds": stats.storage_manager_failed_rounds,
        "timing_scope": "aggregate execution total/max; no per-round percentile or per-stage work attribution",
        "active_workload": "append and optional overwrite; no TTL workload",
        "path": "all-shards scheduler, compact execution reports",
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Vec<String> {
        ["probe", "seed-run", "unused", "string", "128", "2", "3", "1023", "1048576"]
            .into_iter().map(String::from).collect()
    }

    #[test]
    fn values_distinguish_keys_and_overwrite_generations() {
        assert_eq!(value(0, false).len(), 64);
        assert_ne!(value(0, false), value(1, false));
        assert_ne!(value(0, false), value(0, true));
        assert_eq!(value(123, true), value(123, true));
    }

    #[test]
    fn keys_are_fixed_width_and_distinct() {
        let unique = (0..10_000).map(key).collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), 10_000);
        assert!(unique.iter().all(|key| key.len() == 16));
    }

    #[test]
    fn overwrite_selection_includes_zero_only_when_enabled() {
        let mut configured = arguments(&args());
        assert!(overwritten(&configured, 0));
        assert!(!overwritten(&configured, 1));
        assert!(overwritten(&configured, 3));
        configured.overwrite_every = 0;
        assert!(!overwritten(&configured, 0));
    }

    #[test]
    #[should_panic]
    fn zero_rounds_are_refused() {
        let mut configured = args();
        configured[5] = "0".into();
        arguments(&configured);
    }

    #[test]
    #[should_panic]
    fn unknown_phase_is_refused() {
        let mut configured = args();
        configured[1] = "other".into();
        arguments(&configured);
    }
}
