// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RECLAIM HAS ONE PARTICIPANT, which is why twelve comparison fields are out of scope.
//!
//! # THE TWELVE, AND WHAT THEY PRESUPPOSE
//!
//! Twelve names in the storage-metric comparison describe a reclaim coordinated across chunk
//! servers: endpoints to enumerate and disable, control fences, route-state fence matching, plan and
//! raft-entry checksums, trim flow rate limits, and per-node budget and pressure counts.
//!
//! Every one presupposes **multiple reclaim participants**. This store's reclaim has one: a shard
//! owning local slabs. There is nothing for those fields to report, and filling them with degenerate
//! values — one endpoint meaning self, fencing always false, a plan checksum over a plan nobody else
//! sees — would produce fields that *look* like the format and mean nothing. That is the defect this
//! whole surface has been corrected for: a figure carrying a name that does not describe its own
//! arithmetic.
//!
//! # WHY THAT NEEDS A TEST AND NOT A PARAGRAPH
//!
//! "Out of scope" was an assertion in a pull-request body, and the premise under it is a **property
//! of the code**. If reclaim ever gains a second participant, those twelve stop being out of scope
//! and nothing would say so.
//!
//! So the premise is pinned here. `block_store/gc.rs` imports exactly `super::*` and contains none
//! of the constructs a remote participant needs. The day that changes, this fails and names what
//! came into scope with it.
//!
//! Same instrument as its two neighbours: `a_point_read_fills_the_physical_tier` pins a declined
//! design so it cannot return silently, `the_comparison_counterparts_still_exist` pins a mapping so
//! it cannot become fiction, and this pins a scope boundary so it cannot go stale.
//!
//! # WHAT IT DOES NOT CLAIM
//!
//! Not that the store is single-node — it plainly is not, and replication, raft and a shared store
//! all contact peers. The claim is narrower and is the one the exclusion rests on: **the block
//! reclaim path itself** decides and performs reclamation without contacting anybody.
//!
//! rust-internal: reads this crate's own source, no product behaviour

/// The twelve this premise excludes, listed so the failure message can name them.
const EXCLUDED_BY_SINGLE_PARTICIPANT: &[&str] = &[
    "candidate_chunk_endpoints",
    "best_reclaim_candidate_endpoint",
    "skipped_disabled_chunk_endpoints",
    "chunk_control_fenced",
    "block_route_state_fence_matched",
    "expected_plan_checksum",
    "prepared_raft_entry_checksum",
    "trim_flow_max_ops_per_second",
    "trim_flow_throttled_requests",
    "effective_max_trim_ops",
    "reclaim_budget_limited_node_count",
    "reclaim_pressure_node_count",
];

/// Constructs the block reclaim path would need in order to involve a second participant. Chosen to
/// be what a remote call ACTUALLY requires rather than words that merely sound remote: a socket, an
/// http client, an async boundary, a thread, or a channel.
const REMOTE_PARTICIPANT_CONSTRUCTS: &[(&str, &str)] = &[
    ("reqwest", "an http client"),
    ("TcpStream", "a socket"),
    ("UdpSocket", "a socket"),
    (".await", "an async boundary, which a local synchronous walk does not need"),
    ("tokio::", "an async runtime"),
    ("thread::spawn", "work handed to another thread"),
    ("mpsc::", "a channel to another worker"),
    ("rpc", "a remote procedure call"),
    ("Endpoint", "a named participant other than this shard"),
    ("peer", "a named participant other than this shard"),
];

#[test]
fn the_reclaim_path_has_one_participant() {
    let gc = include_str!("../../block_store/gc.rs");

    println!();
    println!("  THE BLOCK RECLAIM PATH, CHECKED FOR A SECOND PARTICIPANT");
    println!();
    println!("    block_store/gc.rs: {} lines", gc.lines().count());
    println!();

    let mut found: Vec<&str> = Vec::new();
    for (needle, what) in REMOTE_PARTICIPANT_CONSTRUCTS {
        let hits = gc.matches(needle).count();
        println!("    {:<16} {:>3}   {what}", needle, hits);
        if hits > 0 {
            found.push(needle);
        }
    }
    println!();

    // ---- THE FILE MUST ACTUALLY BE THE RECLAIM PATH. A guard over an empty or moved file would
    // ---- pass by finding nothing, which is the one way this arm could be vacuous.
    assert!(
        gc.lines().count() > 200,
        "block_store/gc.rs is only {} lines. This arm asserts an ABSENCE, so it passes trivially \
         over a file that has been emptied or had the reclaim logic moved elsewhere -- in which \
         case the premise has to be re-established where that logic now lives",
        gc.lines().count()
    );
    assert!(
        gc.contains("delayed_destroy") || gc.contains("reclaim"),
        "block_store/gc.rs no longer mentions delayed destruction or reclaim, so it is probably no \
         longer the reclaim path and this arm is checking the wrong file"
    );

    // ---- AND IT MUST HAVE NO SECOND PARTICIPANT.
    assert!(
        found.is_empty(),
        "the block reclaim path now contains {found:?}, which a reclaim involving only this shard \
         does not need.\n\
         \n\
         TWELVE FIELDS OF THIS COMPARISON ARE EXCLUDED ON THE PREMISE THAT RECLAIM HAS ONE \
         PARTICIPANT: {EXCLUDED_BY_SINGLE_PARTICIPANT:?}\n\
         \n\
         Each presupposes endpoints to enumerate and disable, fences, plan checksums, or per-node \
         budgets. If reclaim is now coordinated with anything else, that premise has changed and \
         those twelve are in scope -- so decide them deliberately rather than leaving this arm \
         relaxed. If the construct is local (a thread used purely for parallelism within this \
         shard, say), narrow the needle and say why in its entry."
    );

    println!("    no second participant: the twelve endpoint, fence, checksum, plan and");
    println!("    trim-flow fields have nothing to report here, and that is why they are");
    println!("    excluded rather than filled with degenerate values.");
    println!();
}
