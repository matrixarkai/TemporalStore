// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A RESTART AFTER THE SHIPPED LOOP HAS RUN MUST FIND EVERY VALUE, WHETHER OR NOT THE LOG HOLDS IT.
//!
//! The reclaim floor is the lowest of the live registrations and the placements the retained dump
//! manifests name. A restart can also load the persisted served index, which the loop rewrites on
//! its own cadence, and that image names placements too. This drives the loop itself, with the
//! index flushed between rounds, a dump cap that leaves buckets undumped, and rewrites that
//! supersede earlier placements, then reopens over the same files and reads every key.
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
        table_name: "loop-restart".to_string(),
        shard_uri: "local://loop-restart/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load shard 1: {:?}", response.status);
    engine
}

fn drive(async_storage: bool) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    if async_storage {
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 2,
                        async_storage: true,
                        ..Default::default()
                    },
                })
                .ok
        );
    }
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
                command: Command::StringSet {
                    key: key_at(item),
                    value: value_for(item, generation),
                },
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
    for round in 1..=8usize {
        let from = (round * 70) % KEYS;
        let to = (from + 150).min(KEYS);
        write(from, to, round);
        for item in from..to {
            latest[item] = round;
        }
        runtime.run_storage_manager_once(1, options.clone());
        runtime.inner.engine.flush_shard_index(1);
    }
    // NON-VACUITY: the loop really did reclaim some log, or the test is not about reclaim.
    let records_left = runtime
        .inner
        .engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .map(|records| records.len())
        .unwrap_or(usize::MAX);
    let written = KEYS + 8 * 150;
    assert!(
        records_left < written,
        "the log still holds all {written} records, so nothing was reclaimed"
    );
    drop(runtime);

    let reopened = open(dir.path());
    for item in 0..KEYS {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key_at(item) },
        });
        match response.response {
            CommandResponse::Bytes { value: Some(bytes) } => assert_eq!(
                bytes,
                value_for(item, latest[item]),
                "key {item} came back with a stale value"
            ),
            other => panic!("key {item} is missing after the restart: {other:?} {:?}", response.status),
        }
    }
}

#[test]
fn a_restart_after_loop_rounds_with_index_flushes_finds_every_value_in_sync_mode() {
    drive(false);
}

#[test]
fn a_restart_after_loop_rounds_with_index_flushes_finds_every_value_in_async_mode() {
    drive(true);
}
