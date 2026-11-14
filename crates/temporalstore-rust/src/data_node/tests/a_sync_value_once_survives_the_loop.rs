// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SYNCHRONOUS WRITE THAT KEEPS ITS VALUE ONLY IN THE LOG MUST STAY READABLE WHILE THE SHIPPED
//! LOOP RUNS, AND AFTER A RESTART.
//!
//! The same shape as `a_restart_after_loop_rounds_finds_every_value`, with the single-copy
//! placement on and one more assertion: after every round the LIVE engine must answer every key
//! with its latest value, before any restart. A soak through the server showed a third of the keys
//! reading back missing while it ran, so the live read is checked each round and the failing round
//! is named.
#![allow(clippy::all)]
use super::*;

const KEYS: usize = 600;

fn key_at(item: usize) -> String {
    format!("loopkey-{item:05}")
}

fn value_for(item: usize, generation: usize) -> Vec<u8> {
    format!("value-{item:05}-g{generation}-{}", "x".repeat(96)).into_bytes()
}

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "loop-once".to_string(),
        shard_uri: "local://loop-once/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load shard 1: {:?}", response.status);
    engine
}

fn missing_live(engine: &TemporalEngine, latest: &[usize]) -> Vec<usize> {
    // Drop every cached copy first: with the cache warm a read never needs the log, which is how a
    // value whose record was freed can still read back for as long as the cache holds it.
    engine.cache().invalidate_shard(1).unwrap();
    (0..KEYS)
        .filter(|item| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: key_at(*item) },
            });
            !matches!(&response.response,
                CommandResponse::Bytes { value: Some(bytes) } if *bytes == value_for(*item, latest[*item]))
        })
        .collect()
}

#[test]
fn a_single_copy_sync_write_stays_readable_across_loop_rounds_and_a_restart() {
    let _on = crate::engine::value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    );
    let write = |from: usize, to: usize, generation: usize| {
        for item in from..to {
            let response = runtime.inner.engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: key_at(item), value: value_for(item, generation) },
            });
            assert!(response.status.ok, "write {item}: {:?}", response.status);
        }
    };
    let options = StorageManagerOptions {
        min_undumped_wal_bytes: 8_000,
        max_dump_buckets_per_round: 20,
        ..StorageManagerOptions::default()
    };
    write(0, KEYS, 0);
    let mut latest = vec![0usize; KEYS];
    // NON-VACUITY: the values really are log-resident, or this is the two-copy placement.
    assert!(runtime.inner.engine.synthetic_address_count_for_test(1) > 0);
    for round in 1..=8usize {
        let from = (round * 70) % KEYS;
        let to = (from + 150).min(KEYS);
        write(from, to, round);
        for item in from..to {
            latest[item] = round;
        }
        let before = (
            runtime.inner.engine.registration_count_for_test(1),
            runtime.inner.engine.wal_resident_block_count(1),
            runtime.inner.engine.synthetic_address_count_for_test(1),
        );
        let wal_len = |engine: &TemporalEngine| engine.write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX).map(|r| r.len()).unwrap_or(0);
        let floor = |engine: &TemporalEngine| engine.write_ahead_log_store().block_retention_floor(1);
        println!("round {round}: start: log records {}, retention floor {:?}", wal_len(&runtime.inner.engine), floor(&runtime.inner.engine));
        runtime.run_storage_manager_once(1, options.clone());
        println!("round {round}: after loop stage: log records {}, retention floor {:?}", wal_len(&runtime.inner.engine), floor(&runtime.inner.engine));
        runtime.inner.engine.flush_shard_index(1);
        println!("round {round}: after flush: log records {}, retention floor {:?}", wal_len(&runtime.inner.engine), floor(&runtime.inner.engine));
        let after = (
            runtime.inner.engine.registration_count_for_test(1),
            runtime.inner.engine.wal_resident_block_count(1),
            runtime.inner.engine.synthetic_address_count_for_test(1),
        );
        let records = runtime.inner.engine.write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX).map(|r| r.len()).unwrap_or(0);
        println!("round {round}: (registrations, placements, synthetic) before {before:?} after {after:?}, log records {records}");
        let missing = missing_live(&runtime.inner.engine, &latest);
        if let Some(first) = missing.first() {
            let got = runtime.inner.engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: key_at(*first) },
            });
            println!("round {round}: first bad key {first}: got {:?} status {:?}, wanted generation {}",
                got.response, got.status, latest[*first]);
        }
        assert!(
            missing.is_empty(),
            "round {round}: {} of {KEYS} keys read back wrong or missing while the server ran, first {:?}",
            missing.len(),
            &missing[..missing.len().min(5)]
        );
    }
    drop(runtime);

    let reopened = open(dir.path());
    let missing = missing_live(&reopened, &latest);
    assert!(missing.is_empty(), "{} keys wrong or missing after the restart, first {:?}",
        missing.len(), &missing[..missing.len().min(5)]);
}
