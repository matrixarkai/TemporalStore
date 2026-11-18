// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WRITE RATE ABOVE THE PER-ROUND MATERIALISE CAP MUST NOT GROW THE LOG-RESIDENT SET WITHOUT BOUND.
//!
//! With the value only in its log record, every synchronous set holds a registration, and the lowest
//! registration pins the log's retention floor. The loop moves at most `max_dump_buckets_per_round`
//! of them per pass. Written faster than that, the set grows and the log can never be reclaimed.
#![allow(clippy::all)]
use super::*;

const ROUNDS: usize = 10;
const PER_ROUND: usize = 400;
const CAP: usize = 20;

// Ignored until the loop drains log-resident values at the write rate (or the cap is lifted). It is
// the condition for turning `TS_SYNC_VALUE_ONCE` on by default; run it with `-- --ignored`.
#[test]
#[ignore = "the condition for default-on: the log-resident set must stay bounded under sustained writes"]
fn log_resident_values_stay_bounded_when_writes_outrun_the_per_round_cap() {
    let _on = crate::engine::value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "cap".to_string(),
        shard_uri: "local://cap/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(loaded.status.ok);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 64, max_background_queue_depth: 8 },
    );
    let options = StorageManagerOptions {
        min_undumped_wal_bytes: 8_000,
        max_dump_buckets_per_round: CAP,
        ..StorageManagerOptions::default()
    };
    let mut resident_after = Vec::new();
    for round in 0..ROUNDS {
        for n in 0..PER_ROUND {
            let item = round * PER_ROUND + n;
            let response = runtime.inner.engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("capkey-{item:06}"),
                    value: format!("v-{item:06}-{}", "x".repeat(96)).into_bytes(),
                },
            });
            assert!(response.status.ok);
        }
        runtime.run_storage_manager_once(1, options.clone());
        runtime.inner.engine.flush_shard_index(1);
        let resident = runtime.inner.engine.wal_resident_block_count(1);
        let records = runtime.inner.engine.write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX).map(|r| r.len()).unwrap_or(0);
        println!(
            "round {round}: written {} resident {resident} registrations {} log records {records} floor {:?}",
            (round + 1) * PER_ROUND,
            runtime.inner.engine.registration_count_for_test(1),
            runtime.inner.engine.write_ahead_log_store().block_retention_floor(1),
        );
        resident_after.push(resident);
        // The invariant holds at the end of EVERY round, not only the last.
        assert!(
            resident <= CAP,
            "round {round}: {resident} log-resident values after the round, more than the cap {CAP};              history {resident_after:?}"
        );
    }
    // One round's writes plus one round's cap is the most a bounded design may hold at a round's end.
    let bound = PER_ROUND + CAP;
    assert!(
        *resident_after.last().unwrap() <= bound,
        "log-resident values grew without bound: {resident_after:?} after rounds of {PER_ROUND} sets with a cap of {CAP}; \
         the registry and the log floor never advance"
    );
}
