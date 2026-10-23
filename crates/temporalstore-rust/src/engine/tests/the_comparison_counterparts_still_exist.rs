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
//! # THE ONE THAT IS GENUINELY UNRESOLVED
//!
//! `retained_window_compacted_count` has no counterpart here, and is recorded as unresolved rather
//! than rounded away. Their own name for it appears only as a string key in a flat `fields` map, so
//! even its definition could not be read — which is its own useful fact: a name in a metric
//! dictionary is not a schema field, and 14 of the names in this surface are that.
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
const UNRESOLVED: &[(&str, &str)] = &[(
    "retained_window_compacted_count",
    "no retention-window compaction count exists here, and the name appears there only as a key in \
     a flat fields map, so its definition could not be read either",
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
        UNRESOLVED.len() <= 2,
        "{} entries are recorded unresolved. That list exists to name the genuine exceptions, not \
         to absorb anything inconvenient -- if it is growing, the comparison is being abandoned \
         rather than maintained",
        UNRESOLVED.len()
    );
}
