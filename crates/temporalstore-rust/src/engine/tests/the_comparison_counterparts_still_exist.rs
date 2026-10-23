// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE COMPARISON MAPPING, AS A TEST RATHER THAN A DOCUMENT.
//!
//! # WHY
//!
//! This storage-metric comparison has been re-derived by hand three times and came out differently
//! each time — because the answer depends entirely on *how* you compare. Matching by field name
//! against one struct gives one number; matching by the QUANTITY each field measures, against the
//! struct that actually owns that kind of figure, gives a very different one.
//!
//! Each re-derivation was cheap to get wrong and expensive to redo, and **a document cannot fail
//! when the code moves underneath it.** So the mapping lives here, and this asserts every
//! counterpart field still exists where the mapping says it does.
//!
//! # WHICH FAILURE THIS CATCHES, AND WHICH THE COMPILER ALREADY DID
//!
//! Checked by planting both, because "it would catch a rename" was the first thing claimed here and
//! it was wrong:
//!
//!   * **RENAMING** a counterpart does not reach this arm — every one of these fields has other
//!     users, so `cargo check` fails first with `no field named ...`. The compiler is the guard
//!     there, and this arm is redundant for that case.
//!   * **REMOVING** one together with its users **compiles cleanly** — the quantity silently stops
//!     being reported, every other test stays green, and only this arm fails. Planted exactly that
//!     on `best_compaction_candidate_ratio_ppm`: zero compile errors, and this arm failed naming
//!     the entry and its pairing note.
//!
//! That second case is the one a hand-written comparison has no defence against: nothing breaks, and
//! the mapping quietly becomes fiction.
//!
//! It checks **source**, not behaviour. It cannot verify two quantities mean the same thing — that
//! judgement is written beside each entry and is only as good as the reading behind it.
//!
//! # THE THREE THAT ARE GENUINELY UNRESOLVED, EACH FOR A DIFFERENT REASON
//!
//! Recorded rather than rounded away, because *why* a figure is absent is more useful than the fact:
//!
//!   * `retained_window_compacted_count` — `compaction_runs` is a **lifetime** count and
//!     `compaction_watermark` a **position**, so a count *within* a retained window needs runs
//!     tagged by position, which this store does not keep.
//!   * `skipped_disabled_chunk_endpoints` — `enabled` comes from a **per-cycle parameter** the
//!     caller passes, so there is no persistent per-shard setting to count.
//!   * `reclaim_budget_limited_node_count` — `budget_exhausted` is a **transient round outcome**
//!     nothing retains, so reporting it from the status would mean inventing retained state for it.
//!
//! The last two are *not reportable from the status*, which is different from missing: their right
//! home is the cycle report path, where the outcome lives.
//!
//! All three of their names appear in that surface only as string keys in a flat `fields` map, so
//! none of their definitions could be read either — which is its own useful fact: **a name in a
//! metric dictionary is not a schema field**, and 14 of the names here are that.
//!
//! rust-internal: reads this crate's own source, no product behaviour

/// (what it answers, our file, our field or pair, a note on the pairing)
const COUNTERPARTS: &[(&str, &str, &str, &str)] = &[
    (
        "records eligible for compaction before a budget cut in",
        "engine/reports.rs",
        "removable_records_before_budget",
        "the eligible count, which is what a compactable count is before the round is bounded",
    ),
    (
        "records a compaction round actually removed",
        "engine/reports.rs",
        "records_removed",
        "on the index-log GC report: the log is where a change record lives here",
    ),
    (
        "records a transaction-log compaction removed",
        "data_node.rs",
        "index_log_records_removed",
        "the same quantity reported on the GC response rather than the stage report",
    ),
    (
        "tombstones a compaction could drop",
        "engine/reports.rs",
        "delete_marked_object_ids_before",
        "delete-marked ids are this tree's tombstones; `_after` beside it gives the dropped count",
    ),
    (
        "tombstones a compaction did drop",
        "engine/reports.rs",
        "delete_marked_object_ids_after",
        "the second half of that pair -- before minus after is the drop",
    ),
    (
        "the best candidate's recoverable bytes",
        "control.rs",
        "best_compaction_candidate_bytes",
        "the first candidate a ratio-ordered pipeline would take is the best one",
    ),
    (
        "the best candidate's discard ratio",
        "control.rs",
        "best_compaction_candidate_ratio_ppm",
        "recoverable over the slab's live-plus-pending bytes, which is what a min-ratio gate reads",
    ),
    (
        "ranges on the candidate slabs",
        "control.rs",
        "compaction_candidate_range_count",
        "the count beside the bytes: across how many holes the recovery is spread",
    ),
    (
        "candidate slabs emitted by a reclaim pass",
        "control.rs",
        "compaction_candidate_slabs",
        "how many slabs a pass would offer, which is what an emitted-candidate count is",
    ),
    (
        "whether the round was cut short by its budget",
        "engine/reports.rs",
        "budget_exhausted",
        "paired with blocks_left_by_budget and bytes_left_by_budget on the compaction report",
    ),
    (
        "blocks and bytes a budget left behind",
        "engine/reports.rs",
        "blocks_left_by_budget",
        "the skipped-by-budget quantity, named for what was left rather than what was skipped",
    ),
];

/// Named, and deliberately not mapped.
const UNRESOLVED: &[(&str, &str)] = &[
    (
        "skipped_disabled_chunk_endpoints",
        "a count of participants with reclaim disabled. `StorageIndexGcReport::enabled` is assigned \
         from `request.enable_index_gc` -- a PER-CYCLE PARAMETER the caller passes -- so there is no \
         persistent per-shard setting to count. Not reportable from the status, as opposed to \
         missing",
    ),
    (
        "reclaim_budget_limited_node_count",
        "a count of participants whose reclaim was cut short. `budget_exhausted` is computed inside \
         a round and carried out in that round's report; nothing retains it, since `index_gc_report` \
         is RETURNED rather than stored. Reporting it from the status would mean inventing retained \
         state for a transient outcome, which reads zero after a reload -- the staleness hazard the \
         stale record already documents. Its right home is the cycle report path",
    ),
    (
    "retained_window_compacted_count",
    "`compaction_runs` is a LIFETIME count and `compaction_watermark` is a position, so a count of \
     compactions WITHIN a retained window needs runs tagged by position -- which this store does \
     not keep. Their own name appears only as a key in a flat fields map, so its definition could \
     not be read either. A specific absence rather than a missing field",
)];

fn source_of(relative: &str) -> &'static str {
    match relative {
        "control.rs" => include_str!("../../control.rs"),
        "data_node.rs" => include_str!("../../data_node.rs"),
        "engine/reports.rs" => include_str!("../reports.rs"),
        other => panic!("the mapping names a file this arm cannot read: {other}"),
    }
}

#[test]
fn the_comparison_counterparts_still_exist() {
    println!();
    println!("  THE COMPARISON MAPPING, CHECKED AGAINST SOURCE");
    println!();

    let mut checked = 0_usize;
    for (answers, file, field, note) in COUNTERPARTS {
        let source = source_of(file);
        let declared = format!("pub {field}: ");
        let present = source.contains(&declared);
        println!("    {:<52} {file}::{field}", if present { "ok" } else { "MISSING" });
        assert!(
            present,
            "the comparison maps \"{answers}\" onto `{field}` in {file}, and no `pub {field}:` is \
             declared there any more.\n\
             \n\
             Pairing note: {note}\n\
             \n\
             This mapping is the only record of which of their figures this tree already answers. \
             If the field was renamed, update the entry; if it was removed, that quantity is no \
             longer reported and the comparison has genuinely regressed -- either way the mapping \
             has to move with the code, which is why it is a test."
        );
        checked += 1;
    }

    println!();
    for (name, why) in UNRESOLVED {
        println!("    unresolved: {name}");
        println!("      {why}");
    }
    println!();
    println!(
        "    {checked} counterparts pinned, {} recorded unresolved",
        UNRESOLVED.len()
    );
    println!();

    // ---- THE MAPPING MUST NOT BE EMPTY, which is the one way this arm could pass vacuously.
    assert!(
        checked >= 11,
        "only {checked} counterparts are pinned. The mapping is the deliverable here, so a short \
         list means entries were dropped rather than that the comparison shrank"
    );

    // ---- AND THE UNRESOLVED LIST IS NOT A PLACE TO HIDE THINGS. One entry, named, with a reason.
    assert!(
        UNRESOLVED.len() <= 3,
        "{} entries are recorded unresolved. That list names the genuine exceptions and must not \
         absorb anything inconvenient.\n\
         \n\
         THE BOUND WENT 2 -> 3 DELIBERATELY, for two fields whose values are not shard state at \
         all: one is a caller's per-cycle parameter, the other a transient round outcome nothing \
         retains. Those are reasons. The next entry owes its own -- a reason being a statement \
         about WHERE the value lives or why it cannot exist, not that it has not been done yet",
        UNRESOLVED.len()
    );
}
