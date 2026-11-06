// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// THE TEST-MODULE LIST. Nothing but module declarations and the comments that describe them.
//
// This file is `merge=union` in `.gitattributes`: when two branches each add a module, git keeps
// BOTH lines instead of raising a conflict. That is only safe while this file stays a flat list --
// union keeps both sides of an EDITED line too, so a declaration edited on two branches becomes a
// duplicate `mod`, which is a compile error rather than silent corruption. Put code, `use` items
// or anything with structure in `engine_regression_tests.rs`, never here.
//
// This file is `include!`d by `engine_regression_tests.rs` and is NOT a module of its own.

#[path = "tests/engine_tests_conformance.rs"]
mod conformance;
#[path = "tests/concurrent_commit.rs"]
mod concurrent_commit;
#[path = "tests/raft_apply_coalesce.rs"]
mod raft_apply_coalesce;
#[path = "tests/phase1_flat.rs"]
mod phase1_flat;
#[path = "tests/storage_maintenance_and_compaction.rs"]
mod part1;
#[path = "tests/cache_tiers_and_command_execution.rs"]
mod part2;
#[path = "tests/engine_tests_quota.rs"]
mod quota;
#[path = "tests/stream_reads_and_shard_controls.rs"]
mod part3;
#[path = "tests/persistence_recovery_and_accounting.rs"]
mod part4;
#[path = "tests/expiry_scale.rs"]
mod expiry_scale;
#[path = "tests/a_container_write_asks_its_own_slot.rs"]
mod a_container_write_asks_its_own_slot;
#[path = "tests/reclaim_dump.rs"]
mod reclaim_dump;
#[path = "tests/claim_stamp_hold.rs"]
mod claim_stamp_hold;
#[path = "tests/command_surface.rs"]
mod command_surface;
#[path = "tests/expiry_command_boundaries.rs"]
mod expiry_command_boundaries;
#[path = "tests/address_footprint.rs"]
mod address_footprint;
#[path = "tests/address_word_footprint.rs"]
mod address_word_footprint;
#[path = "tests/serving_read_guard.rs"]
mod serving_read_guard;
#[path = "tests/poison_shard_guard.rs"]
mod poison_shard_guard;
#[path = "tests/restore_serving_gate.rs"]
mod restore_serving_gate;
#[path = "tests/page_and_block_counter_ratio.rs"]
mod page_and_block_counter_ratio;
#[path = "tests/page_and_block_name_one_tier.rs"]
mod page_and_block_name_one_tier;
#[path = "tests/manifest_restore_model_maps.rs"]
mod manifest_restore_model_maps;
#[path = "tests/prune_crash_window.rs"]
mod prune_crash_window;
#[path = "tests/install_on_load_anchor.rs"]
mod install_on_load_anchor;
#[path = "tests/index_log_replay_reach.rs"]
mod index_log_replay_reach;
#[path = "tests/round_scale.rs"]
mod round_scale;
#[path = "tests/round_stage_cost.rs"]
mod round_stage_cost;
#[path = "tests/round_backlog_cost.rs"]
mod round_backlog_cost;
#[path = "tests/round_walk_scope.rs"]
mod round_walk_scope;
#[path = "tests/bucket_filing_range.rs"]
mod bucket_filing_range;
#[path = "tests/inner_range.rs"]
mod inner_range;
#[path = "tests/shard_carried_range.rs"]
mod shard_carried_range;
#[path = "tests/shard_carried_identity.rs"]
mod shard_carried_identity;
#[path = "tests/index_bytes_per_key.rs"]
mod index_bytes_per_key;
#[path = "tests/where_an_objects_refs_could_live.rs"]
mod where_an_objects_refs_could_live;
#[path = "tests/sibling_handles.rs"]
mod sibling_handles;
#[path = "tests/level_two_value.rs"]
mod level_two_value;
#[path = "tests/log_resident_is_derived.rs"]
mod log_resident_is_derived;
#[path = "tests/fold_hash_map_completeness.rs"]
mod fold_hash_map_completeness;
#[path = "tests/recorded_map_invariant.rs"]
mod recorded_map_invariant;
#[path = "tests/set_install_census.rs"]
mod set_install_census;
#[path = "tests/model_number_agreement.rs"]
mod model_number_agreement;
#[path = "tests/model_slot_is_a_position.rs"]
mod model_slot_is_a_position;
#[path = "tests/dump_scale.rs"]
mod dump_scale;
#[path = "tests/dump_release_scale.rs"]
mod dump_release_scale;
#[path = "tests/index_write_amplification.rs"]
mod index_write_amplification;
#[path = "tests/restore_scale.rs"]
mod restore_scale;
#[path = "tests/restore_large_scale.rs"]
mod restore_large_scale;
#[path = "tests/promote_precondition.rs"]
mod promote_precondition;
#[path = "tests/alloc_class_scale.rs"]
mod alloc_class_scale;
#[path = "tests/evict_scale.rs"]
mod evict_scale;
#[path = "tests/reclaiming_space_is_its_own_pass.rs"]
mod reclaiming_space_is_its_own_pass;
#[path = "tests/cache_invalidation_cost.rs"]
mod cache_invalidation_cost;
#[path = "tests/cache_read_cost.rs"]
mod cache_read_cost;
#[path = "tests/warm_read_decomposition.rs"]
mod warm_read_decomposition;
#[path = "tests/wal_reclaim_frame_boundary.rs"]
mod wal_reclaim_frame_boundary;
#[path = "tests/blocks_per_bucket.rs"]
mod blocks_per_bucket;
#[path = "tests/bucket_fill.rs"]
mod bucket_fill;
#[path = "tests/routing_range_default.rs"]
mod routing_range_default;
#[path = "tests/context_node_survives_reload.rs"]
mod context_node_survives_reload;
#[path = "tests/durable_outranks_derived.rs"]
mod durable_outranks_derived;
#[path = "tests/per_item_byte_budget.rs"]
mod per_item_byte_budget;
#[path = "tests/flat_block_list.rs"]
mod flat_block_list;
#[path = "tests/range_free_bucket_id.rs"]
mod range_free_bucket_id;
#[path = "tests/container_member_shadow.rs"]
mod container_member_shadow;
#[path = "tests/element_ordinal_reuse.rs"]
mod element_ordinal_reuse;
#[path = "tests/block_entry_names.rs"]
mod block_entry_names;
#[path = "tests/block_entry_handles.rs"]
mod block_entry_handles;
#[path = "tests/bucket_sequence_budget.rs"]
mod bucket_sequence_budget;
#[path = "tests/bucket_node_arms.rs"]
mod bucket_node_arms;
#[path = "tests/bucket_node_container_header.rs"]
mod bucket_node_container_header;
#[path = "tests/bucket_flag_masks.rs"]
mod bucket_flag_masks;
#[path = "tests/the_dirty_buckets_are_a_set_the_map_keeps.rs"]
mod the_dirty_buckets_are_a_set_the_map_keeps;
#[path = "tests/inline_arm_trade.rs"]
mod inline_arm_trade;
#[path = "tests/layout_arm_selection.rs"]
mod layout_arm_selection;
#[path = "tests/entry_object_identity.rs"]
mod entry_object_identity;
#[path = "tests/bucket_tombstone_budget.rs"]
mod bucket_tombstone_budget;
#[path = "tests/entry_count_versus_block_count.rs"]
mod entry_count_versus_block_count;
#[path = "tests/block_entry_name_pointer.rs"]
mod block_entry_name_pointer;
#[path = "tests/bucket_mark_in_place_budget.rs"]
mod bucket_mark_in_place_budget;
#[path = "tests/bucket_dirty_share_under_dumps.rs"]
mod bucket_dirty_share_under_dumps;
#[path = "tests/model_map_container_cost.rs"]
mod model_map_container_cost;
#[path = "tests/container_block_ordinal.rs"]
mod container_block_ordinal;
#[path = "tests/what_a_new_container_element_scans.rs"]
mod what_a_new_container_element_scans;
#[path = "tests/block_entry_element_naming.rs"]
mod block_entry_element_naming;
#[path = "tests/index_resident_truth.rs"]
mod index_resident_truth;
#[path = "tests/carried_block_identity.rs"]
mod carried_block_identity;
#[path = "tests/campaign_end_to_end.rs"]
mod campaign_end_to_end;
#[path = "tests/length_answer_and_listing_agree.rs"]
mod length_answer_and_listing_agree;
#[path = "tests/set_listing_block_reads.rs"]
mod set_listing_block_reads;
#[path = "tests/object_is_a_key_not_an_element.rs"]
mod object_is_a_key_not_an_element;
#[path = "tests/container_block_element_key.rs"]
mod container_block_element_key;
#[path = "tests/the_entry_cannot_name_its_object_by_slot.rs"]
mod the_entry_cannot_name_its_object_by_slot;
#[path = "tests/the_entry_cannot_point_at_the_object_list.rs"]
mod the_entry_cannot_point_at_the_object_list;
#[path = "tests/resident_map_readers.rs"]
mod resident_map_readers;
#[path = "tests/container_blocks_are_batched.rs"]
mod container_blocks_are_batched;
#[path = "tests/view_rebuild_block_read_failures.rs"]
mod view_rebuild_block_read_failures;
#[path = "tests/folded_block_membership.rs"]
mod folded_block_membership;
#[path = "tests/container_tombstone_collection.rs"]
mod container_tombstone_collection;
#[path = "tests/container_tombstone_entry.rs"]
mod container_tombstone_entry;
#[path = "tests/tombstone_reload_path.rs"]
mod tombstone_reload_path;
#[path = "tests/the_dump_door_reads_the_index_stamp.rs"]
mod the_dump_door_reads_the_index_stamp;
#[path = "tests/no_door_delivers_a_sequence_map.rs"]
mod no_door_delivers_a_sequence_map;
#[path = "tests/hash_container_reload_authority.rs"]
mod hash_container_reload_authority;
#[path = "tests/decoded_index_packing.rs"]
mod decoded_index_packing;
#[path = "tests/two_level_resident_shape.rs"]
mod two_level_resident_shape;
#[path = "tests/adopting_the_ported_index_log_record.rs"]
mod adopting_the_ported_index_log_record;
#[path = "tests/reconcile_allocation.rs"]
mod reconcile_allocation;
#[path = "tests/hash_read_union_divergence.rs"]
mod hash_read_union_divergence;
// `packed_element_scan_cost` WAS DECLARED HERE AND THE FILE IS DELETED.
//
// It priced two arms in nanoseconds at occupancies from one element to four hundred: ARM A, the
// per-element level of the resident lookup -- `ObjectBlockRefs::position`, a binary search whose
// comparison dereferenced an `Option<Arc<str>>` into a separate allocation and compared the name
// -- against ARM B, `select_container_element`'s packed linear scan. Its own words: "Arm A is the
// cost that goes; arm B is the cost that stays."
//
// ARM A IS GONE, so the trade it measured is settled rather than open. `ObjectBlockRefs`,
// `ComponentList` and `ComponentBlocks` are deleted and an object maps straight to its
// `BlockRefs`; there is no per-element level left to price and no `position` to call.
//
// IT COULD NOT BE REPAIRED MECHANICALLY, AND ITS OWN DOC SAID NOT TO TRY. The fixture built the
// per-element level BY HAND -- `occupancy` distinct `Some(name)` entries under one object, a state
// the producer could not reach even before the collapse, since `insert_object_block_lookup` wrote
// `component: None` as a literal. And on keeping arm B alone it is explicit: "A measurement that
// priced B alone would be pricing the status quo." Arm B is the shipped selector and both read
// doors already call it unconditionally, so nothing it measured about the scan has changed.
//
// WHAT IT ESTABLISHED, KEPT ON THE RECORD because it is the reasoning the collapse rests on: the
// recorded 3.0-17.8x that read as a refutation was a ratio over a different denominator -- a scan
// of one of 1,024 fixed routing buckets, whose length grows with the corpus -- while the scan it
// was being compared against walks the elements of ONE OBJECT, whose length is a property of the
// data shape and not of the corpus. The two numbers answer different questions and the first never
// bounded the second.
//
// THE ELEMENT-NAME BRANCH'S LAST CHANGE TO IT WAS A WIDTH RESTATEMENT, AND THAT IS WHY THIS
// DELETION IS A MERGE DECISION AND NOT ONLY THIS BRANCH'S. `ea2b26bb7` restated one pin inside
// `resident_bytes_per_object` -- `size_of::<BlockIndex>()` from 56 to 40, with a note that the
// arithmetic MULTIPLIES it so a stale width scales the whole column. That pin's subject survives;
// the arithmetic it fed does not, because the column priced the chain `ObjectBlockRefs ->
// ComponentList -> ComponentBlocks -> BlockRefs` per object and three of those four are deleted.
// The 40 is not lost with the file: it is a `const _` beside `BlockIndex` in `engine_storage_state.rs` and is
// re-asserted in `block_entry_handles`, `blocks_per_bucket`, `per_item_byte_budget`,
// `block_entry_element_naming` and `the_entry_cannot_name_its_object_by_slot`.
#[path = "tests/set_listing_source.rs"]
mod set_listing_source;
#[path = "tests/projection_names_a_block.rs"]
mod projection_names_a_block;
#[path = "tests/authority_check_cost.rs"]
mod authority_check_cost;
#[path = "tests/sweep_keeps_the_collapse.rs"]
mod sweep_keeps_the_collapse;
#[path = "tests/ordinal_under_the_gate.rs"]
mod ordinal_under_the_gate;
#[path = "tests/gated_removal.rs"]
mod gated_removal;
#[path = "tests/replay_under_the_gate.rs"]
mod replay_under_the_gate;
#[path = "tests/removal_the_index_can_find.rs"]
mod removal_the_index_can_find;
#[path = "tests/gated_listing_folds_the_blocks.rs"]
mod gated_listing_folds_the_blocks;
#[path = "tests/the_slot_survives_a_reload.rs"]
mod the_slot_survives_a_reload;
#[path = "tests/the_handle_is_computed_at_the_bucket.rs"]
mod the_handle_is_computed_at_the_bucket;
#[path = "tests/gated_corpus_across_a_store_boundary.rs"]
mod gated_corpus_across_a_store_boundary;
#[path = "tests/write_after_fold.rs"]
mod write_after_fold;
#[path = "tests/collapse_for_list.rs"]
mod collapse_for_list;
#[path = "tests/zset_component_drops_the_score.rs"]
mod zset_component_drops_the_score;
#[path = "tests/entry_width_at_each_step.rs"]
mod entry_width_at_each_step;
#[path = "tests/an_assigned_durable_id_defeats_ground_one_only.rs"]
mod an_assigned_durable_id_defeats_ground_one_only;
#[path = "tests/where_the_characters_are_durable_per_kind.rs"]
mod where_the_characters_are_durable_per_kind;
#[path = "tests/what_an_assigned_id_costs_the_object_lookup.rs"]
mod what_an_assigned_id_costs_the_object_lookup;
#[path = "tests/what_the_core_index_puts_on_the_wire.rs"]
mod what_the_core_index_puts_on_the_wire;
#[path = "tests/the_write_path_leaves_the_bucket_ttl_alone.rs"]
mod the_write_path_leaves_the_bucket_ttl_alone;
#[path = "tests/marking_an_object_dirty_reaches_its_blocks.rs"]
mod marking_an_object_dirty_reaches_its_blocks;
#[path = "tests/one_key_one_allocation.rs"]
mod one_key_one_allocation;
#[path = "tests/string_keyed_holders.rs"]
mod string_keyed_holders;
#[path = "tests/one_record_per_object.rs"]
mod one_record_per_object;
#[path = "tests/the_expiry_pair_shares_one_allocation.rs"]
mod the_expiry_pair_shares_one_allocation;
#[path = "tests/one_key_two_kinds.rs"]
mod one_key_two_kinds;
#[path = "tests/context_node_map_arrival.rs"]
mod context_node_map_arrival;
#[path = "tests/the_third_door_stays_shut.rs"]
mod the_third_door_stays_shut;
#[path = "tests/context_node_arrival_with_no_base_index.rs"]
mod context_node_arrival_with_no_base_index;
#[path = "tests/where_the_control_state_values_can_live.rs"]
mod where_the_control_state_values_can_live;
#[path = "tests/the_consolidated_object_map.rs"]
mod the_consolidated_object_map;
#[path = "tests/the_fold_reaching_loaders_stay_gated.rs"]
mod the_fold_reaching_loaders_stay_gated;
#[path = "tests/no_signature_names_the_series.rs"]
mod no_signature_names_the_series;
#[path = "tests/a_field_that_stops_being_written.rs"]
mod a_field_that_stops_being_written;
#[path = "tests/one_map_for_both_distinct_shapes.rs"]
mod one_map_for_both_distinct_shapes;
#[path = "tests/the_write_back_door_and_the_block_ordinal.rs"]
mod the_write_back_door_and_the_block_ordinal;
#[path = "tests/what_a_series_representation_costs_per_key.rs"]
mod what_a_series_representation_costs_per_key;
#[path = "tests/the_event_series_on_the_owned_array.rs"]
mod the_event_series_on_the_owned_array;
#[path = "tests/the_object_keyed_field_count_is_checked.rs"]
mod the_object_keyed_field_count_is_checked;
#[path = "tests/the_side_table_codec_resolves_shape_from_spelling.rs"]
mod the_side_table_codec_resolves_shape_from_spelling;
#[path = "tests/the_record_machinery_is_not_wired.rs"]
mod the_record_machinery_is_not_wired;
#[path = "tests/the_payload_on_all_three_media.rs"]
mod the_payload_on_all_three_media;
#[path = "tests/how_many_keys_a_cached_frame_serves.rs"]
mod how_many_keys_a_cached_frame_serves;
#[path = "tests/what_the_frame_layer_saves_under_eviction.rs"]
mod what_the_frame_layer_saves_under_eviction;
#[path = "tests/the_live_and_stale_shares_are_a_partition.rs"]
mod the_live_and_stale_shares_are_a_partition;
#[path = "tests/the_published_stale_record_accounts_for_itself.rs"]
mod the_published_stale_record_accounts_for_itself;
#[path = "tests/what_the_durable_byte_encodings_cost.rs"]
mod what_the_durable_byte_encodings_cost;
#[path = "tests/the_anchor_digest_shrinks_and_still_loads.rs"]
mod the_anchor_digest_shrinks_and_still_loads;
#[path = "tests/a_point_read_fills_the_physical_tier.rs"]
mod a_point_read_fills_the_physical_tier;
#[path = "tests/the_payload_is_cached_under_one_key.rs"]
mod the_payload_is_cached_under_one_key;
#[path = "tests/what_the_two_cache_keys_are.rs"]
mod what_the_two_cache_keys_are;
#[path = "tests/what_a_cache_key_costs.rs"]
mod what_a_cache_key_costs;
#[path = "tests/what_a_synchronous_write_leaves_pending.rs"]
mod what_a_synchronous_write_leaves_pending;
#[path = "tests/the_block_bit_answers_the_dirty_bucket_question.rs"]
mod the_block_bit_answers_the_dirty_bucket_question;
#[path = "tests/the_best_candidate_is_at_least_the_average.rs"]
mod the_best_candidate_is_at_least_the_average;
#[path = "tests/the_comparison_counterparts_still_exist.rs"]
mod the_comparison_counterparts_still_exist;
#[path = "tests/the_reclaim_path_has_one_participant.rs"]
mod the_reclaim_path_has_one_participant;
#[path = "tests/a_space_ratio_whose_numerator_falls.rs"]
mod a_space_ratio_whose_numerator_falls;
#[path = "tests/every_published_ratio_is_rederivable.rs"]
mod every_published_ratio_is_rederivable;
#[path = "tests/the_index_log_under_rewrite.rs"]
mod the_index_log_under_rewrite;
#[path = "tests/what_the_collector_sees_after_a_rewrite.rs"]
mod what_the_collector_sees_after_a_rewrite;
#[path = "tests/the_amplification_set_under_rewrite.rs"]
mod the_amplification_set_under_rewrite;
#[path = "tests/the_write_window_closes_at_compaction.rs"]
mod the_write_window_closes_at_compaction;
#[path = "tests/what_a_live_user_bytes_denominator_would_cost.rs"]
mod what_a_live_user_bytes_denominator_would_cost;
#[path = "tests/the_published_set_against_its_envelope.rs"]
mod the_published_set_against_its_envelope;
#[path = "tests/the_user_byte_write_ratio.rs"]
mod the_user_byte_write_ratio;
#[path = "tests/whether_a_dead_slab_can_be_reclaimed.rs"]
mod whether_a_dead_slab_can_be_reclaimed;
#[path = "tests/the_overwrite_records_what_it_retired.rs"]
mod the_overwrite_records_what_it_retired;
#[path = "tests/the_cached_payload_is_a_byte_string.rs"]
mod the_cached_payload_is_a_byte_string;
#[path = "tests/the_payload_cache_per_user_byte.rs"]
mod the_payload_cache_per_user_byte;
#[path = "tests/the_repack_brings_no_table_into_being.rs"]
mod the_repack_brings_no_table_into_being;
#[path = "tests/what_one_eviction_round_writes.rs"]
mod what_one_eviction_round_writes;
#[path = "tests/what_an_absent_key_costs.rs"]
mod what_an_absent_key_costs;
#[path = "tests/the_two_eviction_arms_disagree_on_the_dirty_count.rs"]
mod the_two_eviction_arms_disagree_on_the_dirty_count;
#[path = "tests/a_deferred_checkpoint_still_reloads_with_the_keys_dropped.rs"]
mod a_deferred_checkpoint_still_reloads_with_the_keys_dropped;
#[path = "tests/what_one_visibility_publish_writes.rs"]
mod what_one_visibility_publish_writes;
#[path = "tests/a_deferred_publish_checkpoint_still_reloads_with_the_published_values.rs"]
mod a_deferred_publish_checkpoint_still_reloads_with_the_published_values;
#[path = "tests/what_one_shard_load_writes.rs"]
mod what_one_shard_load_writes;
#[path = "tests/whether_a_bucket_can_count_its_own_dirty_objects.rs"]
mod whether_a_bucket_can_count_its_own_dirty_objects;
#[path = "tests/what_the_mutation_counter_and_the_dirty_container_each_count.rs"]
mod what_the_mutation_counter_and_the_dirty_container_each_count;
#[path = "tests/which_record_the_dirty_bucket_consumers_read.rs"]
mod which_record_the_dirty_bucket_consumers_read;
#[path = "tests/whether_the_reclaim_checks_can_read_the_bucket_node.rs"]
mod whether_the_reclaim_checks_can_read_the_bucket_node;
#[path = "tests/the_index_log_retain_floor.rs"]
mod the_index_log_retain_floor;
#[path = "tests/where_an_object_keys_characters_are_written.rs"]
mod where_an_object_keys_characters_are_written;
#[path = "tests/what_the_slot_index_is_made_of.rs"]
mod what_the_slot_index_is_made_of;
#[path = "tests/the_key_a_block_is_filed_under_inverts_exactly.rs"]
mod the_key_a_block_is_filed_under_inverts_exactly;
#[path = "tests/what_the_slot_index_floor_is.rs"]
mod what_the_slot_index_floor_is;
#[path = "tests/what_each_maintenance_bound_costs_per_round.rs"]
mod what_each_maintenance_bound_costs_per_round;
#[path = "tests/what_one_hash_field_write_costs_durably.rs"]
mod what_one_hash_field_write_costs_durably;
#[path = "tests/a_reconstruct_prunes_the_refs_it_invalidates.rs"]
mod a_reconstruct_prunes_the_refs_it_invalidates;
#[path = "tests/a_windowed_aggregate_must_be_current_and_bounded.rs"]
mod a_windowed_aggregate_must_be_current_and_bounded;
