// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE EXPORTED STORAGE TERMS: they sum across shards, and no ratio is exported at all.
//!
//! # WHY THE ABSENCE IS THE HARD HALF
//!
//! `apply_shard_storage_metrics` aggregates with `saturating_add`, so it SUMS ACROSS SHARDS. A byte
//! total sums correctly. A parts-per-million ratio does not: ten shards at 1.000x would export
//! 10,000,000 ppm, which is not a ratio of anything.
//!
//! That is not hypothetical. Three lines from where the storage terms are added sits this, about a
//! key that shipped broken:
//!
//! ```text
//! // NOT `storage.bucket_entries`: that is the routing RANGE, so this metric reported
//! // u32::MAX per shard -- summed across shards -- regardless of the real index size.
//! ```
//!
//! and the note on its fix adds **"Nothing asserted it, which is why it stayed that way."** This is
//! that assertion.
//!
//! # ASSERTED BY NAME PATTERN, NOT BY A LIST
//!
//! Any exported key ending `_ppm`, or containing `amplification` or `ratio`, fails here. A list of
//! today's ratio names would pass the moment someone adds tomorrow's — which is exactly how the
//! `bucket_entries` key survived.
//!
//! # AND THE SUM IS CHECKED OVER TWO SHARDS WITH DIFFERENT VALUES
//!
//! One shard cannot tell "summed" from "overwritten by the last shard": the two agree at n=1. That
//! is the shape of a per-shard figure that looks right until a second shard exists.
//!
//! rust-internal: drives this crate's own metric exporter, no product behaviour

use std::collections::BTreeMap;

fn shard_with(shard_id: crate::types::ShardId, scale: u64) -> crate::meta::ServerShardServingState {
    crate::meta::ServerShardServingState {
        shard_id,
        storage: crate::control::ShardCanonicalStorageStats {
            live_block_bytes: 100 * scale,
            non_purged_physical_bytes: 500 * scale,
            logical_bytes_written: 700 * scale,
            stale_bytes_recorded: 400 * scale,
            stale_bytes_discarded: 150 * scale,
            pending_discard_bytes: 250 * scale,
            pending_discard_block_refs: 7 * scale,
            stale_bytes_by_overwrite: 300 * scale,
            stale_bytes_by_removal: 70 * scale,
            stale_bytes_by_sweep: 30 * scale,
            discard_candidate_bytes: 90 * scale,
            discard_candidate_slabs: 2 * scale,
            compaction_candidate_bytes: 60 * scale,
            compaction_candidate_slabs: 3 * scale,
            compaction_candidate_range_count: 11 * scale,
            merged_discard_range_count: 13 * scale,
            fragmented_range_count: 5 * scale,
            // SELECTIVE, so the node-level count below can tell "counts the qualifying
            // shards" from "counts every shard". 4 and 0 also keeps the byte-sum
            // assertion exact: 4 + 0 is the same 4 that 1 x scale summed to.
            dropped_discard_range_count: if scale == 1 { 4 } else { 0 },
            stale_slabs_tracked: 4 * scale,
            stale_slabs_pruned: 1 * scale,
            // THE NODE-LEVEL CONDITIONS, set so exactly ONE of the two shards qualifies on each.
            // A fixture where both qualify cannot tell "counts the qualifying shards" from "counts
            // every shard" -- the two agree at 2.
            compaction_recommended: scale == 1,
            // The ratios are populated too, precisely so that exporting one would be POSSIBLE here
            // and the absence below is a choice this arm can observe rather than a vacuous pass.
            block_space_amplification_ppm: 5_000_000,
            live_bytes_ppm: 200_000,
            stale_bytes_ppm: 800_000,
            discard_backlog_ppm: 625_000,
            recorded_space_amplification_ppm: 3_500_000,
            space_amplification_ppm: 1_000_000,
            ..crate::control::ShardCanonicalStorageStats::default()
        },
        ..crate::meta::ServerShardServingState::default()
    }
}

fn exported(shards: &[crate::meta::ServerShardServingState]) -> BTreeMap<String, u64> {
    let mut metrics = BTreeMap::new();
    super::super::apply_shard_storage_metrics(&mut metrics, shards);
    metrics
}

#[test]
fn the_storage_terms_are_exported_and_sum_across_shards() {
    // TWO SHARDS WITH DIFFERENT VALUES. At n=1, "summed" and "last one wins" agree.
    let both = exported(&[shard_with(1, 1), shard_with(2, 3)]);

    for (key, per_unit) in [
        ("storage_live_block_bytes", 100u64),
        ("storage_non_purged_physical_bytes", 500),
        ("storage_logical_bytes_written", 700),
        ("storage_stale_bytes_recorded", 400),
        ("storage_stale_bytes_discarded", 150),
        ("storage_pending_discard_bytes", 250),
        ("storage_pending_discard_block_refs", 7),
        ("storage_stale_bytes_by_overwrite", 300),
        ("storage_stale_bytes_by_removal", 70),
        ("storage_stale_bytes_by_sweep", 30),
        ("storage_discard_candidate_bytes", 90),
        ("storage_discard_candidate_zones", 2),
        ("storage_compaction_candidate_bytes", 60),
        ("storage_compaction_candidate_zones", 3),
        ("storage_compaction_candidate_range_count", 11),
        ("storage_merged_discard_range_count", 13),
        ("storage_fragmented_range_count", 5),
        ("storage_dropped_discard_range_count", 1),
        ("storage_stale_zones_tracked", 4),
        ("storage_stale_zones_pruned", 1),
    ] {
        // scale 1 + scale 3 = four units of each figure
        let want = per_unit * 4;
        assert_eq!(
            both.get(key).copied(),
            Some(want),
            "{key} exported {:?} where two shards at 1x and 3x should sum to {want}. These are \
             byte and count terms, so the exporter's cross-shard add is the right aggregation -- a \
             value equal to one shard's share means it is being overwritten rather than summed",
            both.get(key)
        );
    }

    // ---- AND THE PLACEMENT CAVEAT IS REACHABLE, which is the one key a dashboard built on the
    // ---- candidate counts needs in order to know whether they are counts or lower bounds.
    assert!(
        both.contains_key("storage_dropped_discard_range_count"),
        "the placement caveat is not exported, so a scrape cannot tell whether the candidate \
         figures beside it are exact"
    );

    // ---- THE NODE-LEVEL COUNTS COUNT SHARDS, and the fixture is built so exactly one of the two
    // ---- qualifies on each condition. A count equal to 2 would mean every shard is being counted
    // ---- rather than the qualifying ones, which a fixture where both qualified could not detect.
    for (key, expected) in [
        ("storage_shards_recommending_compaction", 1u64),
        ("storage_shards_offering_a_compaction_candidate", 2),
        ("storage_shards_with_truncated_placement", 1),
    ] {
        let got = both.get(key).copied().unwrap_or_default();
        assert_eq!(
            got, expected,
            "{key} is {got}, expected {expected} over two shards. These count PARTICIPANTS -- a \
             shard is this store's reclaim participant, where the surface being followed counts \
             chunk servers -- so a value above the shard count means the condition is being counted \
             per slab or per candidate instead, and the figure no longer means what its name says"
        );
        assert!(
            got <= 2,
            "{key} is {got} over 2 shards. A count of shards cannot exceed the number of shards"
        );
    }

    // ---- AND NO IDENTITY IS EXPORTED. `best_reclaim_candidate_endpoint` names WHICH participant
    // ---- holds the best candidate. A shard id has no sum, and neither does a cross-shard maximum,
    // ---- so neither belongs in a dictionary aggregated with `saturating_add`.
    let identities: Vec<&String> = both
        .keys()
        .filter(|key| {
            key.contains("endpoint")
                || key.contains("_id")
                || key.ends_with("_shard")
                || key.contains("best_candidate_shard")
        })
        .collect();
    assert!(
        identities.is_empty(),
        "these exported metrics look like identities rather than quantities: {identities:?}. A \
         shard id or an endpoint name has no meaningful sum across shards, which is what this \
         exporter does to every value -- the same category error as exporting a ratio through it"
    );
}

#[test]
fn a_ratio_is_never_exported_as_a_summed_metric() {
    let both = exported(&[shard_with(1, 1), shard_with(2, 3)]);

    // BY PATTERN, NOT BY A LIST OF TODAY'S NAMES. A list would pass the moment the next ratio is
    // added, which is how the `bucket_entries` key survived long enough to ship.
    let offenders: Vec<&String> = both
        .keys()
        .filter(|key| key.ends_with("_ppm") || key.contains("amplification") || key.contains("ratio"))
        .collect();
    assert!(
        offenders.is_empty(),
        "these exported metrics look like ratios: {offenders:?}. This exporter SUMS ACROSS SHARDS \
         (`saturating_add`), so a parts-per-million figure aggregates into nonsense -- two shards \
         at 1.000x would read 2,000,000 ppm. Export the terms and let the consumer divide; every \
         ratio on the status is recoverable from terms already exported here"
    );

    // ---- AND THE ABSENCE IS A CHOICE, NOT AN ACCIDENT OF THE FIXTURE. The ratios are populated on
    // ---- the shards above, so exporting one was possible and this assertion is not vacuous.
    let shard = shard_with(1, 1);
    assert!(
        shard.storage.block_space_amplification_ppm > 0
            && shard.storage.live_bytes_ppm > 0
            && shard.storage.discard_backlog_ppm > 0,
        "the fixture leaves the ratio fields at zero, so the check above would pass even if the \
         exporter did add them"
    );

    // ---- THE TERMS EACH RATIO NEEDS ARE ALL PRESENT, so "let the consumer divide" is true rather
    // ---- than a way of saying the figure is unavailable.
    for needed in [
        "storage_live_block_bytes",
        "storage_non_purged_physical_bytes",
        "storage_stale_bytes_recorded",
        "storage_pending_discard_bytes",
    ] {
        assert!(
            both.contains_key(needed),
            "{needed} is not exported, so a consumer cannot re-derive the ratios this exporter \
             declines to publish -- which would make declining them a loss rather than a choice"
        );
    }
}
