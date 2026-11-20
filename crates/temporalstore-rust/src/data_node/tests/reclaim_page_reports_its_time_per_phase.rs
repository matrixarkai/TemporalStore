// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A block-collection round reports where its time went, one row per sub-phase, in the same
//! per-stage timing table the loop's stages use. The rows are what an operator reads to tell a
//! collection that is slow because it walks the shard from one that is slow because it waits.
#![allow(clippy::all)]
use super::*;

#[test]
fn a_collection_round_reports_each_of_its_phases() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 4, max_background_queue_depth: 2 },
    );
    let seed = StorageManagerOptions { enable_block_gc: false, ..StorageManagerOptions::default() };
    let mut written = 0usize;
    for _ in 0..6 {
        for index in 0..400usize {
            runtime.engine().execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("phase-{:06}", (written + index) % 100),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += 400;
        runtime.run_storage_manager_once(1, seed.clone());
    }
    // CONTROL: no phase row exists before a collection round has run.
    assert!(
        runtime.stats().storage_manager_stage_timing.keys().all(|name| !name.starts_with("reclaim_page.")),
        "a phase row existed before any collection ran"
    );
    runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    let timing = runtime.stats().storage_manager_stage_timing;
    // The page stage invalidates only the removed slabs (after the collection), so the whole-shard
    // `cache_invalidate` row belongs to callers that ask for it and is not read here.
    for phase in ["manifests", "live_set", "dependency_plan", "store_gc"] {
        let row = timing
            .get(&format!("reclaim_page.{phase}"))
            .unwrap_or_else(|| panic!("no row for phase {phase}: {:?}", timing.keys().collect::<Vec<_>>()));
        assert!(row.rounds_executed >= 1, "phase {phase} never ran: {row:?}");
        assert!(row.max_us <= row.total_us, "phase {phase}: max exceeds total: {row:?}");
    }
}
