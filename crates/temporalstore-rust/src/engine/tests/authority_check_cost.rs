// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE AUTHORITY CHECK COSTS ON THE EXECUTE PATH, GATE OFF AGAINST GATE ON.
//!
//! # THE REGRESSION THIS IS LOOKING FOR
//!
//! `promote_model_maps_to_bucket_index_authority` runs on the request path and asks, per live
//! model-map block, whether the bucket index names it. `contains_object_block_address` requires
//! `page.component.as_deref() == component`, so a projection that files entries with no component
//! while the check asks about a NAMED one would answer "missing" for every element -- and a single
//! missing block rebuilds the whole shard index. **Per execute.**
//!
//! That is a latency regression, not a failing test. No footprint guard would see it, and neither
//! would a correctness one: the index it rebuilds is correct. So it is measured here, with the
//! engine's own counters, before anything is asserted about it.
//!
//! # WHY IT MIGHT ALREADY BE CONSISTENT, WHICH IS THE THING TO FIND OUT
//!
//! The check does not have its own walk: it consumes `visit_model_live_blocks`, the same
//! projection the gate already switches. So under the gate both sides of the comparison may
//! already be speaking about pages rather than elements -- the walk emitting `None` and the index
//! holding `None` -- in which case this step needs no code change at all. Printed rather than
//! predicted.
//!
//! # WHERE IT RUNS, WHICH IS NOT WHERE I FIRST LOOKED
//!
//! It is NOT on the ordinary execute path. Its four production callers are `load_shard`, the single
//! post-WAL-replay fold -- deliberately once, which is "what turns an O(n^2) reload into O(n)" --
//! one further lifecycle path, and the stream-batch arm, which is guarded by a latch so it runs per
//! batch only until a freshly loaded shard has paid one scan.
//!
//! A first fixture here ran twenty ordinary writes and the FLOOR caught it: zero checks in both
//! arms. Without that floor the comparison would have passed as `0 <= 0` and reported "no
//! regression" over a path nothing reached. So the check is driven directly, on an engine-built
//! shard, which is the state it sees on load.
//!
//! # THE INSTRUMENT IS THE ENGINE'S OWN, AND ITS CONTRACT IS FOLLOWED
//!
//! `promote_model_map_check_counts` reports checks run, promotions that REBUILT, and blocks the
//! checks walked. Its own doc states the contract: reset immediately before the call being
//! measured, and read in a single-threaded suite. `PAGES` is there because a walk that visited
//! nothing is cheap for the wrong reason and would pass a cheapness assertion anyway.
//!
//! Each arm gets its OWN engine, so neither can answer out of the other's state -- the discipline
//! learned from a listing measurement that read a full answer out of a response cache when the
//! truth was zero.

#![allow(clippy::all)]
use super::*;

const MEMBERS: usize = 40;
const VALUE_WIDTH: usize = 24;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "authority".to_string(),
        shard_uri: "local://authority/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("a-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// Seed one folded set, re-derive the projection under `gate_on`, then run the authority check
/// ONCE and report what it did.
///
/// The re-derivation comes first on purpose: the check compares the model maps against the index,
/// so both sides have to be speaking the same projection for the answer to mean anything. That is
/// the question -- whether a projection that files entries with no component makes a check that
/// asks about a named one report every element missing.
fn one_check_under(gate_on: bool) -> (u64, u64, u64, bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    // BOTH DIRECTIONS AS VALUES. `remove_var` for the off arm selects the compiled-in default,
    // which is ON, so this would have measured the gated path twice.
    if gate_on {
    } else {
    }

    for index in 0..MEMBERS {
        write_to(
            &engine,
            Command::SetAdd {
                key: "authority/set".to_string(),
                member: member_bytes(index),
            },
        );
    }
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 loaded");
    let (start, end) = shard.routing_range();
    // The index as this projection would derive it, so both sides of the comparison agree about
    // what shape the entries are in.
    crate::engine::storage_bucket_internals::rebuild_bucket_first_index(1, shard, start, end);

    // RESET IMMEDIATELY BEFORE THE CALL BEING MEASURED, which is this instrument's contract.
    crate::engine::storage_bucket_internals::reset_promote_model_map_check_counts();
    let promoted = crate::engine::storage_bucket_internals::
        promote_model_maps_to_bucket_index_authority(1, shard, start, end);
    let (checks, rebuilds, pages) =
        crate::engine::storage_bucket_internals::promote_model_map_check_counts();
    drop(shards);
    drop(engine);
    (checks, rebuilds, pages, promoted)
}

#[test]
fn what_the_authority_check_answers_under_each_projection() {
    println!("\n=== authority check, one call, over a folded set of {MEMBERS} ===");
    println!(
        "  {:>5}  {:>7}  {:>9}  {:>7}  {:>9}",
        "gate", "checks", "rebuilds", "pages", "promoted"
    );

    let mut rows: Vec<(&str, u64, u64, u64, bool)> = Vec::new();
    for (label, gate_on) in [("off", false), ("on", true)] {
        let (checks, rebuilds, pages, promoted) = one_check_under(gate_on);
        println!("  {label:>5}  {checks:>7}  {rebuilds:>9}  {pages:>7}  {promoted:>9}");
        rows.push((label, checks, rebuilds, pages, promoted));
    }

    // THE FLOOR, and it has already earned its place: an earlier fixture here reached the check
    // zero times and the comparison below would have passed over nothing.
    for (label, checks, _, pages, _) in &rows {
        assert_eq!(
            1, *checks,
            "gate {label}: the check ran {checks} times where exactly one call was made"
        );
        assert!(
            *pages > 0,
            "gate {label}: the check walked {pages} model-map blocks, so it was cheap because it \
             visited nothing rather than because it answered quickly"
        );
    }

    let off = &rows[0];
    let on = &rows[1];
    println!(
        "  => promoted: {} gate off, {} gate on; rebuilds {} against {}",
        off.4, on.4, off.2, on.2
    );

    // THE REGRESSION THIS STEP EXISTS TO RULE OUT. If the gated projection makes the check report
    // a missing entry, it rebuilds the whole shard index -- on every load, on every post-replay
    // fold, and on every stream batch until the latch engages. That is latency, not a red test.
    assert!(
        !on.4,
        "the gated projection made the authority check report the index incomplete, so it \
         rebuilt the whole shard index. The check asks whether each live block is NAMED by the \
         index; a projection filing entries with no component against a check asking about a \
         named one reads every element as missing. That is a rebuild per load, per post-replay \
         fold, and per stream batch before the latch -- a latency regression no footprint or \
         correctness guard would catch"
    );
    assert_eq!(
        off.2, on.2,
        "the two projections disagree about whether a rebuild is needed ({} off, {} on)",
        off.2, on.2
    );
}
