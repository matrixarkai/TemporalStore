// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WORD THIS FILE IS NAMED FOR IS BEING RETIRED, AND THIS IS THE ROSTER THAT COUNTS IT DOWN.
//!
//! # WHAT IS BEING RETIRED
//!
//! In live code the word names the NAME OF AN ELEMENT of a container: the field of a hash, the
//! member of a set, the position of a zset or list element, the timestamped name of a feature
//! row. It rides on WAL items, on index-log items, on key-state capture and through some thirty
//! function signatures. The replacement spelling is `element_name` for a value and `ElementName`
//! for a type; the codebase already says `element`, `ElementEntry` and `element_key` beside it.
//!
//! Not every hit is that concept. A filesystem path component is a filesystem path component, and
//! the word also survives, deliberately, as a published JSON key and as a serde alias that lets an
//! older record still decode. The roster below says which is which.
//!
//! # WHAT THE ROSTER IS FOR
//!
//! A count that is only ever read in a pull request body stops being true the day a second branch
//! lands. So the count is a test. Every identifier in the crates' Rust source that contains the
//! word, and every string inside a `serde(..)` attribute that contains it, is tallied per file and
//! compared against [`ROSTER`] in BOTH directions:
//!
//!   * a count that RISES fails, which is what stops a new use of the word appearing while another
//!     branch is deleting the old ones;
//!   * a count that FALLS without the roster moving fails too, which is what makes the roster the
//!     artefact a reviewer reads. A deletion that does not lower its row is a roster that has
//!     started to lie, and the next addition would hide inside the slack.
//!
//! THE ROSTER ONLY SHRINKS. The end state is a roster holding rows of the three permanent classes
//! and nothing of class [`Class::ToRetire`].
//!
//! # WHY IT TALLIES TOKENS AND NOT LINES
//!
//! This guard type has been wrong five times in this repository, and every time for the same
//! reason: it matched TEXT where it meant CODE. One refused a correct change because a doc comment
//! said a quantity was "NOT quantised". So the file is lexed first. Comments of every form are
//! removed, string and character literals are blanked, raw strings are honoured, lifetimes are not
//! mistaken for characters, and block comments nest. What is counted is an identifier that is
//! really in the program, or a string that is really inside a `serde` attribute -- the one place a
//! string is part of the on-disk and on-the-wire contract rather than a message.
//!
//! The scan classifies by what the FILE is, and by nothing inside it. Whole files under
//! `engine/tests/` are pulled in by `#[path]` and carry no `#[cfg(test)]` marker, so a marker-based
//! split would have counted them as production. Here a hit is a hit wherever it sits, and the
//! roster row says what it is.
//!
//! # PROSE IS COUNTED TOO, ON ROWS OF ITS OWN
//!
//! The word in a comment, and the word in a message or a test fixture, are tallied per file under
//! the tokens `//` and `<strings>`. They are never swept into a file's identifier wildcard: a
//! sentence must not be able to hide a use in code, and a use in code must not be able to hide
//! behind a sentence. A prose row that falls is lowered, and one that rises fails, exactly like the
//! rest.
//!
//! What stays on the roster at the end is prose that is ABOUT the old word and cannot say anything
//! else: the comment that says an alias keeps a record written under the long key readable, the
//! fixture that spells the key a published report still carries, the sentence about a filesystem
//! path component. Each says why.
//!
//! NOT COUNTED: THIS FILE. It names the word in its own identifiers and sentences, so it is the one
//! file the walk skips, and the skip is asserted to point at a file that exists.
//!
//! # HOW TO READ A MISMATCH
//!
//! The failure prints the table the tree actually has, in roster syntax. Paste only the rows you
//! meant to move. A row that went UP is a use of the word that should be `element_name`.
#![allow(clippy::all)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// What a roster row is, and therefore whether it is allowed to be the last one standing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// The name of a container element. Every row of this class is work still to do, and the
    /// count of them is the number that has to reach zero.
    ToRetire,
    /// A key the HTTP reports publish. Consumers read the key, so the Rust field is renamed and
    /// the key stays spelled as it is, by an explicit `rename`.
    PublishedKey,
    /// A serde `alias` that lets a record written under the long key still decode. An alias is
    /// DESERIALIZE-ONLY, so it can stay while the field it feeds is renamed.
    LegacyAlias,
    /// A different concept that happens to share the word.
    OtherSense,
}

/// One row: `(file relative to the repository root, token, count, class, reason)`.
///
/// `token` is the identifier itself, or the string content INCLUDING its double quotes for a
/// string inside a `serde` attribute, or `"*"` for every identifier in the file that no other row
/// names. The wildcard is how a file full of one concept is one row while a lone identifier of a
/// different concept still gets a row of its own and a reason.
type Row = (&'static str, &'static str, usize, Class, &'static str);

/// Every use of the word in the crates' Rust source, and the file it is in.
///
/// SORTED BY FILE THEN TOKEN, with the wildcard last in its file, and no row may be zero: a row at
/// zero is deleted rather than kept, because a roster that keeps its zeroes is a list nobody can
/// tell is finished.
const ROSTER: &[Row] = &[
    ("crates/temporalstore-rust/src/alloc_probe.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/bin/sequence_trim_cost_harness.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/bin/upsert_reload_harness.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/bin/upsert_reload_harness.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/block_store.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/context_workflow/context_workflow_regression_tests.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine.rs", "//", 87, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/block_in_wal.rs", "//", 5, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/bucket_dump_manifest_methods.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/bucket_store.rs", "//", 12, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/container_blocks.rs", "//", 72, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/container_blocks.rs", "<strings>", 8, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/container_membership.rs", "//", 5, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_compaction.rs", "//", 20, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_compaction.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_hashing.rs", "//", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_lifecycle.rs", "//", 41, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_lifecycle.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_regression_test_modules.rs", "//", 5, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_regression_test_modules.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_regression_test_modules.rs", "component_is_retired", 1, Class::OtherSense, "this guard, registered under the name of the word it counts down"),
    ("crates/temporalstore-rust/src/engine/engine_reports.rs", "\"component\"", 1, Class::PublishedKey, "the key the published block-index report writes; consumers read it, so the Rust field is renamed and the key is kept by an explicit rename"),
    ("crates/temporalstore-rust/src/engine/engine_reports.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/engine_storage_state.rs", "\"component\"", 1, Class::PublishedKey, "the key a tombstone row is stored under in the index dump; a dump an earlier build wrote must still read, so the key is kept by an explicit rename"),
    ("crates/temporalstore-rust/src/engine/engine_storage_state.rs", "//", 118, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/execute_on_shard.rs", "//", 30, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/hash_field_map.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/hot_block_spill.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/object_record.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/packed_blocks.rs", "//", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/recorded_map.rs", "//", 11, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/recovery_sweep_compact.rs", "//", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/storage_bucket_internals.rs", "//", 120, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/storage_reporting.rs", "//", 8, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/stream_batch_methods.rs", "//", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/a_trim_records_one_boundary.rs", "//", 6, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/a_trim_records_one_boundary.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/address_footprint.rs", "//", 19, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/address_footprint.rs", "<strings>", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/an_assigned_durable_id_defeats_ground_one_only.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/authority_check_cost.rs", "//", 4, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/authority_check_cost.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/block_entry_element_naming.rs", "//", 77, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/block_entry_element_naming.rs", "<strings>", 13, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/block_entry_handles.rs", "//", 50, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/block_entry_handles.rs", "<strings>", 15, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/block_entry_name_pointer.rs", "//", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/block_entry_names.rs", "//", 34, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/block_entry_names.rs", "<strings>", 26, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/blocks_per_bucket.rs", "//", 18, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/bucket_fill.rs", "//", 18, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/bucket_fill.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/bucket_node_arms.rs", "//", 12, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/campaign_end_to_end.rs", "//", 5, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/campaign_end_to_end.rs", "<strings>", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/carried_block_identity.rs", "//", 31, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/carried_block_identity.rs", "<strings>", 8, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/collapse_for_list.rs", "//", 25, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/collapse_for_list.rs", "<strings>", 8, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_block_element_key.rs", "//", 31, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_block_element_key.rs", "<strings>", 21, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_block_ordinal.rs", "//", 40, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_block_ordinal.rs", "<strings>", 6, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_blocks_are_batched.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_member_shadow.rs", "//", 51, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_member_shadow.rs", "<strings>", 6, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_tombstone_collection.rs", "//", 11, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_tombstone_collection.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_tombstone_entry.rs", "//", 12, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/container_tombstone_entry.rs", "<strings>", 6, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/context_node_survives_reload.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/durable_outranks_derived.rs", "//", 84, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/durable_outranks_derived.rs", "<strings>", 14, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/element_ordinal_reuse.rs", "//", 116, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/element_ordinal_reuse.rs", "<strings>", 38, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/entry_count_versus_block_count.rs", "//", 23, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/entry_count_versus_block_count.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/entry_object_identity.rs", "//", 41, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/entry_object_identity.rs", "<strings>", 14, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/expiry_command_boundaries.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/flat_block_list.rs", "//", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/fold_hash_map_completeness.rs", "//", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/fold_hash_map_completeness.rs", "<strings>", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/folded_block_membership.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/folded_block_membership.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/gated_corpus_across_a_store_boundary.rs", "//", 5, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/gated_listing_folds_the_blocks.rs", "//", 4, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/gated_removal.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/hash_container_reload_authority.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/hash_read_union_divergence.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/how_many_keys_a_cached_frame_serves.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/index_bytes_per_key.rs", "//", 32, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/index_bytes_per_key.rs", "<strings>", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/index_resident_truth.rs", "//", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/index_resident_truth.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/index_write_amplification.rs", "components", 1, Class::OtherSense, "std::path::Path::components(), the standard library iterator over a path"),
    ("crates/temporalstore-rust/src/engine/tests/inline_arm_trade.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/layout_arm_selection.rs", "//", 18, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/layout_arm_selection.rs", "<strings>", 10, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/length_answer_and_listing_agree.rs", "//", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/no_signature_names_the_series.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/no_signature_names_the_series.rs", "components", 1, Class::OtherSense, "std::path::Path::components(), the standard library iterator over a path"),
    ("crates/temporalstore-rust/src/engine/tests/object_is_a_key_not_an_element.rs", "//", 24, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/object_is_a_key_not_an_element.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/ordinal_under_the_gate.rs", "//", 21, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/ordinal_under_the_gate.rs", "<strings>", 8, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/per_item_byte_budget.rs", "//", 21, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/persistence_recovery_and_accounting.rs", "//", 100, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/persistence_recovery_and_accounting.rs", "<strings>", 12, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/projection_names_a_block.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/recorded_map_invariant.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/removal_the_index_can_find.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/removal_the_index_can_find.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/replay_under_the_gate.rs", "//", 31, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/replay_under_the_gate.rs", "<strings>", 27, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/resident_map_readers.rs", "//", 27, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/resident_map_readers.rs", "<strings>", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/restore_scale.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/round_scale.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/routing_range_default.rs", "//", 5, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/routing_range_default.rs", "<strings>", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/set_install_census.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/set_listing_block_reads.rs", "//", 29, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/set_listing_block_reads.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/set_listing_source.rs", "//", 4, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/sibling_handles.rs", "//", 8, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/storage_maintenance_and_compaction.rs", "//", 8, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/storage_maintenance_and_compaction.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/stream_reads_and_shard_controls.rs", "//", 5, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/stream_reads_and_shard_controls.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/sweep_keeps_the_collapse.rs", "//", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_entry_cannot_name_its_object_by_slot.rs", "//", 6, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_entry_cannot_name_its_object_by_slot.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_entry_cannot_point_at_the_object_list.rs", "//", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_event_series_on_the_owned_array.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_handle_is_computed_at_the_bucket.rs", "//", 4, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_key_a_block_is_filed_under_inverts_exactly.rs", "//", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_key_a_block_is_filed_under_inverts_exactly.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/the_record_machinery_is_not_wired.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_a_cache_key_costs.rs", "<strings>", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_an_element_name_writes_to_disk_and_the_wire.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_an_element_name_writes_to_disk_and_the_wire.rs", "<strings>", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_the_core_index_puts_on_the_wire.rs", "//", 11, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_the_core_index_puts_on_the_wire.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_the_slot_index_is_made_of.rs", "//", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_the_slot_index_is_made_of.rs", "<strings>", 7, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/what_the_two_cache_keys_are.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/write_after_fold.rs", "//", 18, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/write_after_fold.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/tests/zset_element_name_drops_the_score.rs", "//", 12, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/engine/timestamped_series.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/foreign_type_name_guard.rs", "<strings>", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/foreign_type_name_guard.rs", "components", 2, Class::OtherSense, "std::path::Path::components(), the standard library iterator over a path"),
    ("crates/temporalstore-rust/src/index_log.rs", "//", 31, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/index_log.rs", "<strings>", 3, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/matrixark_rust_proxy_impl.rs", "sanitize_path_component", 3, Class::OtherSense, "a filesystem path component: the directory name a namespace or table is stored under"),
    ("crates/temporalstore-rust/src/proxy.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/proxy/proxy_context.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/raft/wal_proto.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/record_cost.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/shared_store.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/telemetry.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/wal.rs", "\"component\"", 2, Class::LegacyAlias, "alias on the two renamed WAL record fields: a record written under the long key still decodes (an alias is deserialize-only, the written key is the short rename)"),
    ("crates/temporalstore-rust/src/wal.rs", "//", 18, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/wal.rs", "<strings>", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/wal_proto.rs", "//", 18, Class::ToRetire, ""),
    ("crates/temporalstore-rust/src/wal_scale.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/tests/hash_set_delete_recovery.rs", "//", 2, Class::ToRetire, ""),
    ("crates/temporalstore-rust/tests/list_recovery.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/tests/string_delete_recovery.rs", "//", 1, Class::ToRetire, ""),
    ("crates/temporalstore-rust/tests/zset_recovery.rs", "//", 3, Class::ToRetire, ""),
];

/// Files whose NAME contains the word. The module a file declares is counted as an identifier in
/// the list that registers it; the file's own name is counted here.
const FILE_NAMES: &[(&str, Class, &str)] = &[
    (
        "crates/temporalstore-rust/src/engine/tests/component_is_retired.rs",
        Class::OtherSense,
        "this guard, named for the word it counts down",
    ),
];

/// The one file the walk does not read: this one.
const THE_GUARD_ITSELF: &str = "crates/temporalstore-rust/src/engine/tests/component_is_retired.rs";

// ---------------------------------------------------------------------------------------------
// THE LEXER
// ---------------------------------------------------------------------------------------------

/// What a file is made of once comments and string contents are taken out of it.
#[derive(Debug, Default)]
struct Lexed {
    /// The source with every comment removed and every string or character literal blanked to a
    /// single space. Newlines inside what was removed are kept so line numbers still agree.
    code: String,
    /// The content of every string literal that sits inside an attribute whose body names `serde`.
    serde_strings: Vec<String>,
    /// The text of every comment, doc comments and block comments included.
    comments: Vec<String>,
    /// The content of every other string literal: messages, fixtures, and the strings of attributes
    /// that are not `serde` ones (a `#[path = ".."]`, a `cfg` feature name).
    other_strings: Vec<String>,
}

fn is_ident_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

/// Where a string literal that starts at `at` has its content, whether it is raw, and how many
/// `#` close it. `None` when `at` does not begin a string literal.
///
/// Recognises `"`, `b"`, `c"`, and every raw form `r"`, `r#"`, `br"`, `cr##"`. A prefix letter only
/// counts when it is not the tail of a longer identifier, so `for"` and `bar"` are not strings
/// that start early.
fn string_opens_at(c: &[char], at: usize) -> Option<(usize, bool, usize)> {
    if at > 0 && is_ident_char(c[at - 1]) {
        return None;
    }
    let mut i = at;
    if i < c.len() && (c[i] == 'b' || c[i] == 'c') {
        i += 1;
    }
    let raw = i < c.len() && c[i] == 'r';
    if raw {
        i += 1;
    }
    let mut hashes = 0usize;
    if raw {
        while i < c.len() && c[i] == '#' {
            hashes += 1;
            i += 1;
        }
    }
    if i < c.len() && c[i] == '"' {
        Some((i + 1, raw, hashes))
    } else {
        None
    }
}

fn lex(source: &str) -> Lexed {
    let c: Vec<char> = source.chars().collect();
    let n = c.len();
    let mut out = Lexed::default();
    let mut i = 0usize;

    // Attribute state. An attribute opens at `#[` or `#![` and closes at its matching `]`. The
    // strings inside it are held back until it closes, because only then is it known whether the
    // attribute is a `serde` one.
    let mut attr_depth = 0usize;
    let mut attr_code = String::new();
    let mut attr_strings: Vec<String> = Vec::new();

    while i < n {
        let ch = c[i];

        // Line comment, doc comments included. The newline is left for the next turn to emit.
        if ch == '/' && i + 1 < n && c[i + 1] == '/' {
            let start = i;
            while i < n && c[i] != '\n' {
                i += 1;
            }
            out.comments.push(c[start..i].iter().collect());
            continue;
        }

        // Block comment. They nest in Rust, and an unbalanced `/*` inside one is not a close.
        if ch == '/' && i + 1 < n && c[i + 1] == '*' {
            let start = i;
            let mut depth = 1usize;
            i += 2;
            while i < n && depth > 0 {
                if c[i] == '/' && i + 1 < n && c[i + 1] == '*' {
                    depth += 1;
                    i += 2;
                } else if c[i] == '*' && i + 1 < n && c[i + 1] == '/' {
                    depth -= 1;
                    i += 2;
                } else {
                    if c[i] == '\n' {
                        out.code.push('\n');
                    }
                    i += 1;
                }
            }
            out.comments.push(c[start..i.min(n)].iter().collect());
            out.code.push(' ');
            continue;
        }

        // String literal, raw or not.
        if matches!(ch, '"' | 'b' | 'c' | 'r') {
            if let Some((start, raw, hashes)) = string_opens_at(&c, i) {
                let mut j = start;
                let mut text = String::new();
                loop {
                    if j >= n {
                        break;
                    }
                    if raw {
                        if c[j] == '"' && (0..hashes).all(|k| j + 1 + k < n && c[j + 1 + k] == '#') {
                            j += 1 + hashes;
                            break;
                        }
                    } else if c[j] == '\\' && j + 1 < n {
                        text.push(c[j]);
                        text.push(c[j + 1]);
                        if c[j + 1] == '\n' {
                            out.code.push('\n');
                        }
                        j += 2;
                        continue;
                    } else if c[j] == '"' {
                        j += 1;
                        break;
                    }
                    if c[j] == '\n' {
                        out.code.push('\n');
                    }
                    text.push(c[j]);
                    j += 1;
                }
                if attr_depth > 0 {
                    attr_strings.push(text);
                    attr_code.push(' ');
                } else {
                    out.other_strings.push(text);
                }
                out.code.push(' ');
                i = j;
                continue;
            }
        }

        // A quote is a character literal or a lifetime. `'a'` and `'\n'` and `'"'` are the first;
        // `'a` and `'static` are the second and must keep the code after them.
        if ch == '\'' {
            if i + 1 < n && c[i + 1] == '\\' {
                let mut j = i + 3;
                while j < n && c[j] != '\'' {
                    j += 1;
                }
                out.code.push(' ');
                i = (j + 1).min(n);
                continue;
            }
            if i + 2 < n && c[i + 2] == '\'' {
                out.code.push(' ');
                i += 3;
                continue;
            }
        }

        // An attribute opens.
        if ch == '#' && attr_depth == 0 {
            let mut j = i + 1;
            if j < n && c[j] == '!' {
                j += 1;
            }
            if j < n && c[j] == '[' {
                attr_depth = 1;
                attr_code.clear();
                attr_strings.clear();
                out.code.push_str(&c[i..=j].iter().collect::<String>());
                i = j + 1;
                continue;
            }
        }

        if attr_depth > 0 {
            if ch == '[' {
                attr_depth += 1;
            } else if ch == ']' {
                attr_depth -= 1;
                if attr_depth == 0 {
                    let is_serde = attr_code
                        .split(|x: char| !is_ident_char(x))
                        .any(|word| word == "serde");
                    if is_serde {
                        out.serde_strings.append(&mut attr_strings);
                    } else {
                        out.other_strings.append(&mut attr_strings);
                    }
                    attr_strings.clear();
                }
            }
            attr_code.push(ch);
        }

        out.code.push(ch);
        i += 1;
    }
    out
}

/// Every identifier in `code` that contains the word, in any case.
fn identifiers_with_the_word(code: &str) -> Vec<String> {
    code.split(|x: char| !is_ident_char(x))
        .filter(|word| !word.is_empty())
        .filter(|word| word.to_ascii_lowercase().contains("component"))
        .map(str::to_string)
        .collect()
}

/// `(file, token) -> count` for one file's text.
fn tally_text(file: &str, text: &str, into: &mut BTreeMap<(String, String), usize>) {
    let lexed = lex(text);
    for word in identifiers_with_the_word(&lexed.code) {
        *into.entry((file.to_string(), word)).or_default() += 1;
    }
    for string in lexed.serde_strings {
        if string.to_ascii_lowercase().contains("component") {
            *into
                .entry((file.to_string(), format!("\"{string}\"")))
                .or_default() += 1;
        }
    }
    // PROSE, counted apart from code and under tokens that cannot be an identifier. The same word
    // in a comment or in a message is not the same problem as the word in the program, and it is
    // rostered on its own row so that a sentence cannot hide a use in code or the other way round.
    let in_comments: usize = lexed.comments.iter().map(|text| occurrences(text)).sum();
    if in_comments > 0 {
        *into
            .entry((file.to_string(), COMMENT_TOKEN.to_string()))
            .or_default() += in_comments;
    }
    let in_strings: usize = lexed
        .other_strings
        .iter()
        .map(|text| occurrences(text))
        .sum();
    if in_strings > 0 {
        *into
            .entry((file.to_string(), STRING_TOKEN.to_string()))
            .or_default() += in_strings;
    }
}

/// The token a file's comments are rostered under, and the one its non-serde strings are.
const COMMENT_TOKEN: &str = "//";
const STRING_TOKEN: &str = "<strings>";

/// How many times the word occurs in `text`, in any case, not overlapping.
fn occurrences(text: &str) -> usize {
    text.to_ascii_lowercase().matches("component").count()
}

/// An identifier starts with a letter or an underscore. Everything else a row can name -- a serde
/// string, a comment tally, a string tally -- is explicit and is never swept into a wildcard.
fn is_identifier_token(token: &str) -> bool {
    token
        .chars()
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
}

// ---------------------------------------------------------------------------------------------
// THE WALK
// ---------------------------------------------------------------------------------------------

fn repository_root() -> PathBuf {
    // `CARGO_MANIFEST_DIR` is `<root>/crates/temporalstore-rust`.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the crate sits two levels under the repository root")
        .to_path_buf()
}

/// Every `.rs` file under `<root>/crates`, as a path relative to the root with `/` separators.
fn rust_files(root: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("read_dir {} failed: {err}", dir.display()));
        for entry in entries {
            let entry = entry.expect("dir entry readable");
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            let kind = entry.file_type().expect("file type readable");
            if kind.is_dir() {
                if name != "target" && !name.starts_with('.') {
                    stack.push(path);
                }
            } else if kind.is_file() && name.ends_with(".rs") {
                let rel = path
                    .strip_prefix(root)
                    .expect("under the root")
                    .to_string_lossy()
                    .replace('\\', "/");
                found.push(rel);
            }
        }
    }
    found.sort();
    found
}

fn tally_tree() -> (usize, BTreeMap<(String, String), usize>, Vec<String>) {
    let root = repository_root();
    let files = rust_files(&root);
    let mut tally = BTreeMap::new();
    let mut scanned = 0usize;
    for rel in &files {
        if rel == THE_GUARD_ITSELF {
            continue;
        }
        let text = std::fs::read_to_string(root.join(rel))
            .unwrap_or_else(|err| panic!("read {rel} failed: {err}"));
        tally_text(rel, &text, &mut tally);
        scanned += 1;
    }
    (scanned, tally, files)
}

// ---------------------------------------------------------------------------------------------
// THE COMPARISON
// ---------------------------------------------------------------------------------------------

/// Expand the roster to what it claims per `(file, token)`, where the wildcard row of a file is
/// resolved against the identifiers the file's other rows did not name.
fn compare(
    actual: &BTreeMap<(String, String), usize>,
    roster: &[Row],
) -> (Vec<String>, BTreeMap<(String, String), usize>) {
    let mut problems = Vec::new();
    let mut claimed: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut wildcard: BTreeMap<String, usize> = BTreeMap::new();
    for (file, token, count, _, _) in roster {
        if *token == "*" {
            wildcard.insert(file.to_string(), *count);
        } else {
            claimed.insert((file.to_string(), token.to_string()), *count);
        }
    }

    // Resolve the actual tally into explicit tokens and a per-file residual.
    let mut seen_explicit: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut residual: BTreeMap<String, usize> = BTreeMap::new();
    for ((file, token), count) in actual {
        if claimed.contains_key(&(file.clone(), token.clone())) {
            seen_explicit.insert((file.clone(), token.clone()), *count);
        } else if !is_identifier_token(token) {
            // A serde string is never swept into a wildcard: it is the contract, so it is named.
            // The prose tallies are named too, so that a sentence cannot hide a use in code.
            problems.push(format!(
                "{file}: {token} appears {count} time(s) and no row names it"
            ));
        } else {
            *residual.entry(file.clone()).or_default() += count;
        }
    }

    for ((file, token), want) in &claimed {
        let have = seen_explicit
            .get(&(file.clone(), token.clone()))
            .copied()
            .unwrap_or(0);
        if have != *want {
            problems.push(format!(
                "{file}: {token} is rostered at {want} and the tree has {have}{}",
                direction(have, *want)
            ));
        }
    }
    let files: std::collections::BTreeSet<&String> =
        wildcard.keys().chain(residual.keys()).collect();
    for file in files {
        let want = wildcard.get(file).copied().unwrap_or(0);
        let have = residual.get(file).copied().unwrap_or(0);
        if have != want {
            problems.push(format!(
                "{file}: the unnamed identifiers are rostered at {want} and the tree has {have}{}",
                direction(have, want)
            ));
        }
    }
    (problems, seen_explicit)
}

fn direction(have: usize, want: usize) -> &'static str {
    if have > want {
        " -- ROSE: a new use of the word, which should be `element_name`"
    } else {
        " -- FELL: lower the row, because a roster that keeps its slack hides the next addition"
    }
}

/// The table the tree actually has, in roster syntax, for pasting.
fn render_actual(actual: &BTreeMap<(String, String), usize>) -> String {
    let mut by_file: BTreeMap<&str, (Vec<(&str, usize)>, usize)> = BTreeMap::new();
    for ((file, token), count) in actual {
        let slot = by_file.entry(file.as_str()).or_default();
        if !is_identifier_token(token) || token == "sanitize_path_component" {
            slot.0.push((token.as_str(), *count));
        } else {
            slot.1 += count;
        }
    }
    let mut out = String::new();
    // Beside the rows, which tokens make up each file's wildcard: what to work through next.
    let mut tokens_of: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for ((file, token), count) in actual {
        tokens_of
            .entry(file.as_str())
            .or_default()
            .push(format!("{token} x{count}"));
    }
    for (file, tokens) in &tokens_of {
        out.push_str(&format!("    // {file}: {}\n", tokens.join(", ")));
    }
    out.push('\n');
    for (file, (named, residual)) in by_file {
        for (token, count) in named {
            let token = token.replace('"', "\\\"");
            out.push_str(&format!(
                "    (\"{file}\", \"{token}\", {count}, Class::OtherSense, \"DECIDE\"),\n"
            ));
        }
        if residual > 0 {
            out.push_str(&format!(
                "    (\"{file}\", \"*\", {residual}, Class::ToRetire, \"\"),\n"
            ));
        }
    }
    out
}

fn total_of(roster: &[Row], class: Class) -> usize {
    roster
        .iter()
        .filter(|row| row.3 == class)
        .map(|row| row.2)
        .sum()
}

// ---------------------------------------------------------------------------------------------
// THE TESTS
// ---------------------------------------------------------------------------------------------

/// The lexer, on the shapes that have each broken a text-matching guard somewhere.
#[test]
fn the_lexer_reads_code_and_leaves_comments_strings_and_lifetimes_alone() {
    fn hits(src: &str) -> Vec<String> {
        identifiers_with_the_word(&lex(src).code)
    }

    // Plain code is seen, in every case form.
    assert_eq!(hits("let component = 1;"), vec!["component"]);
    assert_eq!(hits("struct ElementComponent;"), vec!["ElementComponent"]);
    assert_eq!(hits("fn f(c: Option<&str>) -> COMPONENT_MAX {}"), vec!["COMPONENT_MAX"]);

    // Line comments, doc comments and inner doc comments are not code.
    assert!(hits("// the component name\nlet x = 1;").is_empty());
    assert!(hits("/// a component\nfn f() {}").is_empty());
    assert!(hits("//! the component\n").is_empty());

    // Block comments, nested, with the close of the inner one not ending the outer.
    assert!(hits("/* outer /* inner component */ still component */ let x = 1;").is_empty());
    assert_eq!(
        hits("/* a /* b */ c */ let component = 1;"),
        vec!["component"],
        "code after a nested block comment is code"
    );

    // Strings: the word in a message is not an identifier.
    assert!(hits(r#"let m = "no component here";"#).is_empty());
    assert!(hits("let m = \"an \\\" escaped quote then component\";").is_empty());
    assert!(hits("let m = \"first line \\\n    component continued\";").is_empty());
    assert_eq!(hits("let m = \"x\"; let component = 1;"), vec!["component"]);

    // Raw strings, with and without hashes, and a quote inside one.
    assert!(hits("let m = r\"component\";").is_empty());
    assert!(hits("let m = r#\"a \"quoted\" component\"#;").is_empty());
    assert!(hits("let m = br##\"component \"# not yet\"##;").is_empty());
    assert_eq!(hits("let m = r#\"x\"#; let component = 1;"), vec!["component"]);

    // A prefix letter that is the tail of an identifier does not open a string early.
    assert_eq!(hits("let bar = f(component);"), vec!["component"]);
    assert_eq!(hits("let r = 1; let component = r;"), vec!["component"]);

    // Character literals and lifetimes.
    assert_eq!(hits("let q = '\"'; let component = 1;"), vec!["component"]);
    assert_eq!(hits("let q = '\\''; let component = 1;"), vec!["component"]);
    assert_eq!(hits("let q = '\\u{1F600}'; let component = 1;"), vec!["component"]);
    assert_eq!(
        hits("fn f<'a>(x: &'a str) -> &'static str { let component = x; \"\" }"),
        vec!["component"],
        "a lifetime must not swallow the code after it"
    );

    // Serde attribute strings are collected, other attributes' strings are not.
    let serde = lex("#[serde(rename = \"c\", alias = \"component\", default)]\npub component: u8,");
    assert_eq!(serde.serde_strings, vec!["c", "component"]);
    assert_eq!(identifiers_with_the_word(&serde.code), vec!["component"]);
    let doc = lex("#[doc = \"a component\"]\nfn f() {}");
    assert!(doc.serde_strings.is_empty(), "a doc attribute is prose");
    let cfg = lex("#[cfg_attr(feature = \"x\", serde(alias = \"component\"))]\nfn f() {}");
    assert_eq!(cfg.serde_strings, vec!["x", "component"]);
    let inner = lex("#![allow(clippy::all)]\nfn f() {}");
    assert!(inner.serde_strings.is_empty());
    // A bracket inside an attribute string does not close the attribute early.
    let bracket = lex("#[serde(rename = \"a]b\", alias = \"component\")]\nlet z = 1;");
    assert_eq!(bracket.serde_strings, vec!["a]b", "component"]);
    assert!(bracket.code.contains("let z = 1;"));

    // A string outside any attribute is never a serde string.
    assert!(lex("let m = \"component\";").serde_strings.is_empty());

    // Comments and strings are collected for the prose tally, each under its own head.
    let prose = lex(
        "// one component\nlet m = \"two component\"; /* three component */ let component = 1;",
    );
    assert_eq!(prose.comments, vec!["// one component", "/* three component */"]);
    assert_eq!(prose.other_strings, vec!["two component"]);
    assert_eq!(identifiers_with_the_word(&prose.code), vec!["component"]);
    // The strings of an attribute that is not a serde one are strings, and not the contract.
    let path = lex("#[path = \"tests/x_component.rs\"]\nmod x;");
    assert_eq!(path.other_strings, vec!["tests/x_component.rs"]);
    assert!(path.serde_strings.is_empty());
    // A nested block comment is one comment.
    let nested = lex("/* a /* b component */ c */ let z = 1;");
    assert_eq!(nested.comments.len(), 1);
}

/// The no-op control. A change that must not move the count does not, and a change that must move
/// it by exactly one does -- on the same text, in the same run.
///
/// THREE CATCHES DO NOT PROVE A HARNESS DISCRIMINATES; A NO-OP THAT REDDENS NOTHING DOES. This is
/// that no-op, held in the test so it runs on every commit instead of once in a pull request.
#[test]
fn a_comment_or_string_edit_moves_nothing_and_a_code_edit_moves_exactly_one() {
    let base = "fn walk(path: &str) -> usize {\n    let n = path.len();\n    n\n}\n";
    // CODE tokens only: identifiers and serde strings. Prose has its own tally, below.
    let count = |text: &str| {
        let mut t = BTreeMap::new();
        tally_text("x.rs", text, &mut t);
        t.iter()
            .filter(|((_, token), _)| token != COMMENT_TOKEN && token != STRING_TOKEN)
            .map(|(_, n)| *n)
            .sum::<usize>()
    };
    assert_eq!(count(base), 0);

    // No-ops.
    assert_eq!(count(&format!("// the component\n{base}")), 0);
    assert_eq!(count(&format!("/// every component\n{base}")), 0);
    assert_eq!(count(&format!("/* component */{base}")), 0);
    assert_eq!(count(&base.replace("path.len()", "path.len() /* component */")), 0);
    assert_eq!(count(&format!("{base}const M: &str = \"component\";\n")), 0);
    assert_eq!(count(&format!("{base}#[doc = \"component\"]\nfn g() {{}}\n")), 0);

    // One identifier in code is exactly one.
    assert_eq!(count(&base.replace("let n", "let component_n")), 1);
    assert_eq!(count(&format!("{base}fn component() {{}}\n")), 1);

    // The same word twice on a line is two, not one.
    assert_eq!(count(&base.replace("n\n}", "component + component\n}")), 2);

    // A serde alias is one, even though it is a string.
    assert_eq!(
        count(&format!("{base}#[serde(alias = \"component\")]\nfn g() {{}}\n")),
        1
    );
}

/// The roster is the tree, in both directions.
#[test]
fn the_roster_matches_the_tree_in_both_directions() {
    let (scanned, actual, files) = tally_tree();

    // --- NON-VACUITY: a clean result must be a fact about the tree and not about the walk. ---
    assert!(
        scanned >= 150,
        "the walk read {scanned} files; that is a broken walk, not a clean tree"
    );
    assert!(
        files
            .iter()
            .any(|f| f.starts_with("crates/temporalstore-rust/tests/")),
        "the walk did not reach the integration tests"
    );
    assert!(
        files
            .iter()
            .any(|f| f.starts_with("crates/temporalstore-snapshot/")),
        "the walk did not reach the second crate"
    );
    assert!(
        files.iter().any(|f| f == THE_GUARD_ITSELF),
        "the one file the walk skips is not at {THE_GUARD_ITSELF}; the skip names nothing"
    );

    // --- THE ROSTER ITSELF IS WELL FORMED. ---
    let mut previous: Option<(&str, &str)> = None;
    for (file, token, count, class, reason) in ROSTER {
        assert!(*count > 0, "{file} {token}: a row at zero is deleted, not kept");
        assert!(
            *class == Class::ToRetire || !reason.is_empty(),
            "{file} {token}: a row of a permanent class says WHY it is permanent"
        );
        if let Some((pf, pt)) = previous {
            let order = |t: &str| (t == "*", t.to_string());
            assert!(
                (pf, order(pt)) < (*file, order(token)),
                "{file} {token} is out of order or a duplicate (after {pf} {pt})"
            );
        }
        previous = Some((*file, *token));
    }

    let (problems, _) = compare(&actual, ROSTER);
    let to_retire = total_of(ROSTER, Class::ToRetire);
    println!(
        "component roster: {to_retire} still to retire; {} published key, {} legacy alias, {} other sense; {scanned} files read",
        total_of(ROSTER, Class::PublishedKey),
        total_of(ROSTER, Class::LegacyAlias),
        total_of(ROSTER, Class::OtherSense),
    );
    assert!(
        problems.is_empty(),
        "the roster and the tree disagree:\n{}\n\nTHE TABLE THE TREE HAS:\n{}",
        problems.join("\n"),
        render_actual(&actual)
    );
}

/// A file named for the word is on the roster, and so is every file the roster names.
#[test]
fn the_files_named_for_the_word_are_the_roster() {
    let root = repository_root();
    let mut named: Vec<String> = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = entry.path();
            let is_dir = entry.file_type().map(|k| k.is_dir()).unwrap_or(false);
            if is_dir {
                if name != "target" && name != "node_modules" && !name.starts_with('.') {
                    stack.push(path);
                }
            } else if name.to_ascii_lowercase().contains("component") {
                named.push(
                    path.strip_prefix(&root)
                        .expect("under the root")
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    named.sort();
    let rostered: Vec<String> = FILE_NAMES.iter().map(|row| row.0.to_string()).collect();
    assert_eq!(
        named, rostered,
        "the files named for the word are not the rostered ones; a rename moves the roster in the \
         same change"
    );
    for (file, class, reason) in FILE_NAMES {
        assert!(
            *class == Class::ToRetire || !reason.is_empty(),
            "{file}: a permanent class says why"
        );
    }
}

/// Prose is tallied apart from code: a sentence moves its own row and nothing else.
#[test]
fn a_sentence_moves_only_its_own_row() {
    let tally = |text: &str| {
        let mut t = BTreeMap::new();
        tally_text("x.rs", text, &mut t);
        t
    };
    let base = "fn f() {}\n";
    assert!(tally(base).is_empty());

    // One comment sentence naming the word twice is two on the comment row and nothing else.
    let commented = tally(&format!("// a component and another Component\n{base}"));
    assert_eq!(commented.len(), 1);
    assert_eq!(commented[&("x.rs".to_string(), COMMENT_TOKEN.to_string())], 2);

    // A block comment, a doc comment: the same row.
    let docs = tally(&format!("/// the COMPONENT\n/* and a component */{base}"));
    assert_eq!(docs[&("x.rs".to_string(), COMMENT_TOKEN.to_string())], 2);

    // A message is on the string row and nowhere else.
    let message = tally(&format!("{base}const M: &str = \"no component here\";\n"));
    assert_eq!(message.len(), 1);
    assert_eq!(message[&("x.rs".to_string(), STRING_TOKEN.to_string())], 1);

    // An identifier is on its own row and moves neither prose row.
    let ident = tally(&format!("{base}fn component() {{}}\n"));
    assert_eq!(ident.len(), 1);
    assert_eq!(ident[&("x.rs".to_string(), "component".to_string())], 1);

    // A serde string is the contract: on its own row, and not also a message.
    let serde = tally(&format!("{base}#[serde(alias = \"component\")]\nfn g() {{}}\n"));
    assert_eq!(serde.len(), 1);
    assert_eq!(serde[&("x.rs".to_string(), "\"component\"".to_string())], 1);

    // The word inside a longer word is still the word, which is what a roster of prose wants:
    // `components` is two syllables of the same problem.
    let plural = tally(&format!("// many components\n{base}"));
    assert_eq!(plural[&("x.rs".to_string(), COMMENT_TOKEN.to_string())], 1);
}
