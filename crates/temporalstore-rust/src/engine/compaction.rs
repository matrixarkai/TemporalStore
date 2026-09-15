// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction utility/policy/layout reporting helpers, split from engine.rs.
use super::*;

/// How many bytes one compaction round may relocate.
///
/// Compaction used to relocate EVERY live page of every model in one pass, holding the shard
/// write lock throughout, so the stall grew with the store. A bound makes the round's cost a
/// property of the constant rather than of the corpus.
///
/// Large enough that a store whose live pages fit inside it still compacts in a single round --
/// which is every store the suite builds, so the existing assertions about what one compaction
/// leaves behind are unchanged. A store bigger than this takes several rounds, and each one
/// relocates pages that have not moved yet rather than re-moving the last round's work.
pub(super) const COMPACTION_ROUND_BYTES: u64 = 256 * 1024 * 1024;

/// How many page refs one compaction round may relocate.
///
/// The stall this bounds tracks the NUMBER of refs moved, not their size: measured in release,
/// a relocation costs ~200 us per ref almost regardless of page size. So a byte budget bounds it
/// only through average page size, and the same byte constant gives wildly different stalls --
/// 256 KiB is ~2,400 refs of 100-byte values but ~64 refs of 4 KiB ones. A ref budget bounds the
/// stall directly.
///
/// 2,048 puts a round near 0.4 s of shard write lock at the measured per-ref cost. It is chosen
/// to sit ABOVE every fixture the suite builds -- which are tens of objects, so they still
/// compact in one round and their single-round assumptions hold -- and BELOW production scale,
/// where 20k refs in one round measured 3.1 s. That is the difference from
/// `COMPACTION_ROUND_BYTES`, which was sized only to clear the suite and so never binds anywhere.
///
/// A round that stops here stays open and the next one resumes onto the same slab, so bounding
/// costs round count, not progress.
pub(super) const COMPACTION_ROUND_BLOCK_REFS: usize = 2_048;

/// Blocks per slab, counted by header walk.
///
/// Both callers below read `page_count` and nothing else off a slab report, and `slab_reports()`
/// reaches that by calling `decode_block_record` on every record in the store -- a CRC32C verify
/// and a decompress each. The compaction phase builds a utility report and a model-layout report
/// BEFORE and AFTER the relocation, so that was four whole-store decodes per round to populate a
/// before/after figure. `count_slab_blocks` walks headers instead: measured 35x cheaper at 32,000
/// records, with identical counts.
///
/// The relocation itself was never the problem -- it has a 256 MiB budget and resumes. This is
/// the survey around it.
fn slab_block_counts_by_slab(block_store: &BlockStore) -> BTreeMap<u64, u64> {
    block_store
        .slab_block_counts()
        .unwrap_or_default()
        .into_iter()
        .map(|(block_slab_id, _physical_bytes, block_count)| (block_slab_id, block_count))
        .collect()
}

pub(super) fn compaction_utility_report(
    block_store: &BlockStore,
    shard: &ShardState,
) -> ShardCompactionUtilityReport {
    compaction_utility_report_from_entries(block_store, shard, &collect_live_block_entries(shard))
}

/// The same report, from live-page entries the caller ALREADY has.
///
/// `collect_live_block_entries` materializes every live page in the shard. The compaction preamble
/// builds several reports from that same set, so one walk can serve them all; the wrapper above
/// keeps the old signature for callers with nothing to share.
pub(super) fn compaction_utility_report_from_entries(
    block_store: &BlockStore,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
) -> ShardCompactionUtilityReport {
    let addresses = entries
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| entry.address.clone())
        .collect::<Vec<_>>();
    let live_block_slab_ids = addresses
        .iter()
        .map(|address| address.block_slab_id)
        .collect::<BTreeSet<_>>();
    let slab_page_counts = slab_block_counts_by_slab(block_store);
    let total_block_count = live_block_slab_ids
        .iter()
        .map(|block_slab_id| {
            slab_page_counts
                .get(block_slab_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let live_block_refs = addresses.len() as u64;
    let stale_block_estimate = total_block_count.saturating_sub(live_block_refs);
    let live_ref_density_basis_points = if total_block_count == 0 {
        0
    } else {
        live_block_refs.saturating_mul(10_000) / total_block_count
    };
    ShardCompactionUtilityReport {
        live_block_slab_count: live_block_slab_ids.len(),
        total_block_count,
        live_block_refs,
        stale_block_estimate,
        live_ref_density_basis_points,
        model_policies: model_compaction_policy_reports(shard, &entries, &slab_page_counts),
    }
}

pub(super) fn model_compaction_policy_reports(
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    slab_page_counts: &BTreeMap<u64, u64>,
) -> Vec<ModelCompactionPolicyReport> {
    #[derive(Default)]
    struct ModelStats {
        live_block_refs: u64,
        deleted_block_refs: u64,
        slab_ids: BTreeSet<u64>,
    }

    let mut by_model = BTreeMap::<String, ModelStats>::new();
    for entry in entries {
        let stats = by_model.entry(entry.kind.clone().to_string()).or_default();
        if entry.deleted {
            stats.deleted_block_refs = stats.deleted_block_refs.saturating_add(1);
        } else {
            stats.live_block_refs = stats.live_block_refs.saturating_add(1);
            stats.slab_ids.insert(entry.address.block_slab_id);
        }
    }
    for key in shard
        .dirty_objects
        .iter()
        .filter(|key| !record_exists(shard, key))
    {
        let model_id = if shard.hashes.contains_key(key) {
            "hash"
        } else if shard.sets.contains_key(key) {
            "set"
        } else if shard.lists.contains_key(key) {
            "list"
        } else if shard.zsets.contains_key(key) {
            "zset"
        } else if shard.features.contains_key(key) {
            "feature"
        } else if shard.control_state_blocks.contains_key(key) {
            "control_state"
        } else if shard.context_nodes.contains_key(key) {
            "context_node"
        } else if split_context_entity_key(key)
            .map(|(collection_key, entity_hash)| {
                shard
                    .context_entities
                    .get(&collection_key)
                    .is_some_and(|series| series.contains_key(&entity_hash))
            })
            .unwrap_or(false)
        {
            "context_entity"
        } else {
            "string"
        };
        let stats = by_model.entry(model_id.to_string()).or_default();
        stats.deleted_block_refs = stats.deleted_block_refs.saturating_add(1);
    }

    by_model
        .into_iter()
        .map(|(model_id, stats)| {
            let total_slab_blocks = stats
                .slab_ids
                .iter()
                .map(|slab_id| {
                    slab_page_counts
                        .get(slab_id)
                        .copied()
                        .unwrap_or_default()
                })
                .sum::<u64>();
            let stale_block_estimate = total_slab_blocks.saturating_sub(stats.live_block_refs);
            let stale_density_basis_points = if total_slab_blocks == 0 {
                0
            } else {
                stale_block_estimate.saturating_mul(10_000) / total_slab_blocks
            };
            let total_refs = stats.live_block_refs.saturating_add(stats.deleted_block_refs);
            let delete_marker_density_basis_points = if total_refs == 0 {
                0
            } else {
                stats.deleted_block_refs.saturating_mul(10_000) / total_refs
            };
            let layout_policy = compaction_layout_policy_for_model(&model_id);
            let stale_density_triggered = stale_density_basis_points > 0;
            let delete_marker_compaction_triggered =
                stats.deleted_block_refs > 0 || delete_marker_density_basis_points > 0;
            let object_block_packing_enabled = compaction_object_block_packing_enabled(&model_id);
            let layout_aware_rewrite_required = object_block_packing_enabled
                || matches!(
                    layout_policy,
                    "timestamped_chunked_pages" | "context_timeline_or_sidecar_pages"
                )
                || model_id == "control_state";
            ModelCompactionPolicyReport {
                layout_policy: layout_policy.to_string(),
                object_block_packing_enabled,
                model_id,
                live_block_refs: stats.live_block_refs,
                deleted_block_refs: stats.deleted_block_refs,
                total_slab_blocks,
                stale_block_estimate,
                stale_density_basis_points,
                delete_marker_density_basis_points,
                object_block_pack_group_count: stats.slab_ids.len() as u64,
                cold_block_rewrite_eligible_refs: stats.live_block_refs,
                compaction_action: compaction_action_for_policy(
                    stats.live_block_refs,
                    stats.deleted_block_refs,
                    stale_density_basis_points,
                    delete_marker_density_basis_points,
                )
                .to_string(),
                stale_density_triggered,
                delete_marker_compaction_triggered,
                layout_aware_rewrite_required,
            }
        })
        .collect()
}

pub(super) fn compaction_layout_policy_for_model(model_id: &str) -> &'static str {
    match model_id {
        "string" | "control_state" | "context_node" | "context_entity" | "context_embedding" => {
            "single_page_object"
        }
        "hash" | "set" => "component_page_object",
        "feature" | "sequence" => "timestamped_chunked_pages",
        model if model.starts_with("context_") => "context_timeline_or_sidecar_pages",
        _ => "generic_page_object",
    }
}

pub(super) fn compaction_object_block_packing_enabled(model_id: &str) -> bool {
    matches!(
        compaction_layout_policy_for_model(model_id),
        "single_page_object" | "component_page_object"
    )
}

pub(super) fn compaction_action_for_policy(
    live_block_refs: u64,
    deleted_block_refs: u64,
    stale_density_basis_points: u64,
    delete_marker_density_basis_points: u64,
) -> &'static str {
    if live_block_refs == 0 && deleted_block_refs > 0 {
        "drop_tombstones"
    } else if delete_marker_density_basis_points > 0 || deleted_block_refs > 0 {
        "rewrite_live_drop_tombstones"
    } else if stale_density_basis_points > 0 {
        "rewrite_stale_density"
    } else {
        "rewrite_cold_or_pack"
    }
}

#[derive(Debug, Default)]
pub(super) struct CompactionRewriteStats {
    pub(super) rewritten_block_refs: usize,
    pub(super) cold_block_rewrite_refs: usize,
    by_model: BTreeMap<String, ModelCompactionRewriteStats>,
    /// The slab this round is filling. A page already there does not move.
    target_block_slab_id: u64,
    /// Bytes this round may still relocate. Saturates at zero, and a round that reaches zero
    /// leaves the rest where it is for the next one.
    budget_bytes: u64,
    /// Page refs this round may still relocate, the bound that actually tracks the stall.
    /// Same saturating behaviour as `budget_bytes`; whichever runs out first ends the round.
    budget_block_refs: usize,
    pub(super) skipped_by_budget: usize,
    pub(super) skipped_by_budget_bytes: u64,
    /// Bytes this round committed to copying, charged at the same point the byte budget is.
    ///
    /// This is the cost of the round stated in the unit that matters. `rewritten_page_refs`
    /// counts relocations; a relocation reads a page and appends it verbatim somewhere else, so
    /// what it actually spends is its LENGTH, and two rounds with the same ref count can differ
    /// by orders of magnitude in what they moved.
    pub(super) relocated_bytes: u64,
    /// The slabs the caller asked this round to empty, or `None` for every live page.
    ///
    /// `None` is what a DIRECT compaction has always meant and still means: the operator RPC
    /// and the on-demand cycle are instructions, not suggestions. `Some` is what the PERIODIC
    /// loop issues, and it names the only slabs a relocation can recover anything for -- the ones
    /// carrying dead space that objects still hold pages on. A page on any OTHER slab comes out
    /// of a relocation byte for byte what it went in as, on a slab that is now the one carrying
    /// the dead space, so moving it recovers nothing and costs a read, an append and a share of
    /// an index record.
    drain_block_slab_ids: Option<BTreeSet<u64>>,
    /// Pages left where they are because emptying their slab was not asked for.
    ///
    /// NOT work left behind, and the distinction is the termination argument: a later round will
    /// not want these either, so this must never keep the round open. That is why
    /// `should_relocate` tests the drain set BEFORE the budget -- charging these to
    /// `skipped_by_budget` would make `left_work_behind` true for ever on any shard with a
    /// page outside the drain set, and the round would never close.
    pub(super) skipped_off_drain_set: usize,
}

#[derive(Debug, Default)]
pub(super) struct ModelCompactionRewriteStats {
    rewritten_block_refs: usize,
    cold_block_rewrite_refs: usize,
}

impl CompactionRewriteStats {
    /// A round that relocates onto `target_block_slab_id`, spending at most `budget_bytes` and
    /// `budget_block_refs`. Whichever runs out first ends the round.
    pub(super) fn for_round(
        target_block_slab_id: u64,
        budget_bytes: u64,
        budget_block_refs: usize,
    ) -> Self {
        Self {
            target_block_slab_id,
            budget_bytes,
            budget_block_refs,
            ..Self::default()
        }
    }

    /// The same round, restricted to the pages sitting on `drain_block_slab_ids`.
    ///
    /// The set comes off the reclaim plan the maintenance round has already built, so asking for
    /// it costs no walk of the shard -- see `compaction_drain_block_slab_ids`. A round built
    /// this way moves exactly the pages the relocation hint names: the ones whose movement
    /// empties a slab, and nothing else.
    pub(super) fn for_drain_round(
        target_block_slab_id: u64,
        budget_bytes: u64,
        budget_block_refs: usize,
        drain_block_slab_ids: BTreeSet<u64>,
    ) -> Self {
        Self {
            drain_block_slab_ids: Some(drain_block_slab_ids),
            ..Self::for_round(target_block_slab_id, budget_bytes, budget_block_refs)
        }
    }

    /// Whether this address should move, charging the budget when it should.
    ///
    /// Three reasons not to move one: it is already on the slab this round is filling, which is
    /// how a resumed round avoids redoing its predecessor's work; its slab is not one this round
    /// was asked to empty, so moving it would recover nothing; or the round has spent its budget,
    /// which is how it stays bounded. The budget is charged BEFORE the page is read, since
    /// reading is the expensive half and the length is known from the address.
    ///
    /// ORDER MATTERS between the last two. A page outside the drain set must not be charged to
    /// `skipped_by_budget`, because that is what `left_work_behind` reads to keep a round
    /// open for the next one -- and a round kept open by pages nobody will ever want moved never
    /// closes.
    pub(super) fn should_relocate(&mut self, address: &BlockAddress) -> bool {
        if address.block_slab_id == self.target_block_slab_id {
            return false;
        }
        if self
            .drain_block_slab_ids
            .as_ref()
            .is_some_and(|drain| !drain.contains(&address.block_slab_id))
        {
            self.skipped_off_drain_set = self.skipped_off_drain_set.saturating_add(1);
            return false;
        }
        if address.length > self.budget_bytes || self.budget_block_refs == 0 {
            self.skipped_by_budget = self.skipped_by_budget.saturating_add(1);
            self.skipped_by_budget_bytes =
                self.skipped_by_budget_bytes.saturating_add(address.length);
            return false;
        }
        self.budget_bytes = self.budget_bytes.saturating_sub(address.length);
        self.budget_block_refs = self.budget_block_refs.saturating_sub(1);
        self.relocated_bytes = self.relocated_bytes.saturating_add(address.length);
        true
    }

    /// Whether the round stopped early, so the engine keeps it open for the next one.
    pub(super) fn left_work_behind(&self) -> bool {
        self.skipped_by_budget > 0
    }

    fn record(&mut self, model_id: &str, cold_block: bool) {
        self.rewritten_block_refs = self.rewritten_block_refs.saturating_add(1);
        let model = self.by_model.entry(model_id.to_string()).or_default();
        model.rewritten_block_refs = model.rewritten_block_refs.saturating_add(1);
        if cold_block {
            self.cold_block_rewrite_refs = self.cold_block_rewrite_refs.saturating_add(1);
            model.cold_block_rewrite_refs = model.cold_block_rewrite_refs.saturating_add(1);
        }
    }

    pub(super) fn into_reports(
        self,
        before: &ShardCompactionUtilityReport,
    ) -> Vec<ModelCompactionRewriteReport> {
        self.by_model
            .into_iter()
            .map(|(model_id, stats)| {
                let before_policy = before
                    .model_policies
                    .iter()
                    .find(|policy| policy.model_id == model_id);
                ModelCompactionRewriteReport {
                    layout_policy: compaction_layout_policy_for_model(&model_id).to_string(),
                    model_id,
                    rewritten_block_refs: stats.rewritten_block_refs,
                    cold_block_rewrite_refs: stats.cold_block_rewrite_refs,
                    object_block_pack_group_count: before_policy
                        .map(|policy| policy.object_block_pack_group_count as usize)
                        .unwrap_or_default(),
                    delete_marker_density_basis_points: before_policy
                        .map(|policy| policy.delete_marker_density_basis_points)
                        .unwrap_or_default(),
                    stale_density_basis_points: before_policy
                        .map(|policy| policy.stale_density_basis_points)
                        .unwrap_or_default(),
                }
            })
            .collect()
    }
}

pub(super) fn block_memory_resident(cache: &MultiLayerCache, shard_id: ShardId, address: &BlockAddress) -> bool {
    cache
        .get_memory(&CacheKey::page_with_slot_generation(
            shard_id,
            address.block_slab_id,
            address.offset,
            address.length,
            address.routing_bucket(),
            address.generation(),
        ))
        .is_some()
}

pub(super) fn compaction_model_layout_reports(
    block_store: &BlockStore,
    shard: &ShardState,
) -> Vec<ShardCompactionModelLayoutReport> {
    let slab_page_counts = slab_block_counts_by_slab(block_store);
    let mut reports = Vec::new();
    reports.push(compaction_layout_from_addresses(
        "string",
        shard.strings.len(),
        shard.strings.values().cloned(),
        &slab_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "hash",
        shard.hashes.len(),
        shard
            .hashes
            .values()
            .flat_map(|fields| fields.values().cloned()),
        &slab_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "zset",
        shard.zsets.len(),
        shard
            .zsets
            .values()
            .flat_map(|members| members.values().map(|(_, address)| address.clone())),
        &slab_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "list",
        shard.lists.len(),
        shard
            .lists
            .values()
            .flat_map(|elements| elements.values().cloned()),
        &slab_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "set",
        shard.sets.len(),
        shard
            .sets
            .values()
            .flat_map(|members| members.values().cloned()),
        &slab_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "feature",
        &shard.features,
        &slab_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_node",
        shard.context_nodes.len(),
        shard.context_nodes.values().cloned(),
        &slab_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_event",
        &shard.context_events,
        &slab_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_index",
        &shard.context_indexes,
        &slab_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_audit",
        &shard.context_audits,
        &slab_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_entity",
        shard.context_entities.values().map(BTreeMap::len).sum(),
        shard
            .context_entities
            .values()
            .flat_map(|series| series.values().cloned()),
        &slab_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_child",
        &shard.context_children,
        &slab_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_summary",
        &shard.context_summaries,
        &slab_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_compression",
        &shard.context_compressions,
        &slab_page_counts,
    ));
    reports.retain(|report| report.object_count > 0 || report.index_refs > 0);
    reports
}

pub(super) fn compaction_timestamped_layout(
    kind: &str,
    timelines: &HashMap<String, BTreeMap<u64, BlockAddress>>,
    slab_page_counts: &BTreeMap<u64, u64>,
) -> ShardCompactionModelLayoutReport {
    let mut ref_counts = HashMap::<BlockAddress, usize>::new();
    for address in timelines
        .values()
        .flat_map(|series| series.values().cloned())
    {
        *ref_counts.entry(address).or_default() += 1;
    }
    let packed_pages = ref_counts.values().filter(|count| **count > 1).count();
    compaction_layout_from_addresses(
        kind,
        timelines.len(),
        ref_counts.keys().cloned(),
        slab_page_counts,
        Some(packed_pages),
    )
    .with_index_refs(ref_counts.values().sum())
}

pub(super) fn compaction_layout_from_addresses(
    kind: &str,
    object_count: usize,
    addresses: impl IntoIterator<Item = BlockAddress>,
    slab_page_counts: &BTreeMap<u64, u64>,
    packed_pages: Option<usize>,
) -> ShardCompactionModelLayoutReport {
    let addresses = addresses.into_iter().collect::<Vec<_>>();
    let unique_addresses = addresses
        .iter()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let live_slab_ids = unique_addresses
        .iter()
        .map(|address| address.block_slab_id)
        .collect::<BTreeSet<_>>();
    let total_blocks_in_live_slabs = live_slab_ids
        .iter()
        .map(|slab_id| {
            slab_page_counts
                .get(slab_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let unique_block_refs = unique_addresses.len();
    let packed_timestamped_blocks = packed_pages.unwrap_or_default();
    let live_ref_density_basis_points = if total_blocks_in_live_slabs == 0 {
        0
    } else {
        (unique_block_refs as u64).saturating_mul(10_000) / total_blocks_in_live_slabs
    };
    ShardCompactionModelLayoutReport {
        kind: kind.to_string(),
        object_count,
        index_refs: addresses.len(),
        unique_block_refs,
        packed_timestamped_blocks,
        legacy_value_blocks: unique_block_refs.saturating_sub(packed_timestamped_blocks),
        stale_block_estimate: total_blocks_in_live_slabs.saturating_sub(unique_block_refs as u64),
        live_ref_density_basis_points,
    }
}

pub(super) trait CompactionLayoutIndexRefs {
    fn with_index_refs(self, index_refs: usize) -> Self;
}

impl CompactionLayoutIndexRefs for ShardCompactionModelLayoutReport {
    fn with_index_refs(mut self, index_refs: usize) -> Self {
        self.index_refs = index_refs;
        self
    }
}

/// Fault seam: make the block read inside a compaction round fail after N relocations.
///
/// The ONLY way a round fails partway in production is `read_block_bytes` returning `None` --
/// a torn or missing page -- and nothing else in the round can produce that state on demand. The
/// resume-anchor behaviour on the partial-failure path is therefore untestable without a seam, so
/// this is it. Thread-local, because a round runs on its caller's thread and tests that set it
/// must not reach a round another test is driving.
#[cfg(test)]
thread_local! {
    static FAIL_BLOCK_READ_AFTER: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Arm (`Some(n)`) or disarm (`None`) the seam above. `Some(0)` fails the first read.
#[cfg(test)]
pub(crate) fn fail_compaction_block_read_after_for_test(relocations: Option<usize>) {
    FAIL_BLOCK_READ_AFTER.with(|cell| cell.set(relocations));
}

/// `read_block_bytes`, with the test seam in front of it. Compiles to a direct call outside
/// tests: the hook exists only under `cfg(test)`, so no production round pays for it.
fn read_block_bytes_for_compaction(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    address: &BlockAddress,
) -> Option<Vec<u8>> {
    #[cfg(test)]
    {
        let armed = FAIL_BLOCK_READ_AFTER.with(|cell| cell.get());
        if let Some(remaining) = armed {
            if remaining == 0 {
                return None;
            }
            FAIL_BLOCK_READ_AFTER.with(|cell| cell.set(Some(remaining - 1)));
        }
    }
    read_block_bytes(cache, block_store, shard_id, address)
}

pub(super) fn compact_block_addresses<'a>(
    block_store: &BlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    addresses: impl IntoIterator<Item = &'a mut BlockAddress>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    for address in addresses {
        if !rewrite_stats.should_relocate(address) {
            continue;
        }
        let cold_block = !block_memory_resident(cache, shard_id, address);
        let bytes = read_block_bytes_for_compaction(cache, block_store, shard_id, address)
            .ok_or_else(|| {
                Status::error(
                    "page_compaction_failed",
                    "missing page bytes during compaction",
                )
            })?;
        // Compaction REWRITES a block that already exists, so it keeps the id that block
        // already has. A block id is an index inside its object: taking a fresh one here would
        // give every block of a multi-block object the same index, and the index entries would
        // collide -- which reads back as a reload losing rows.
        let new_address = block_store
            .append_block_of_object(
                &bytes,
                address.object_id(),
                address.routing_bucket(),
                address.block_id().unwrap_or_default() as u32,
            )
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.block_slab_id,
                new_address.offset,
                new_address.length,
                new_address.routing_bucket(),
                new_address.generation(),
            ),
            bytes,
        );
        rewrite_stats.record(model_id, cold_block);
    }
    Ok(())
}

pub(super) fn compact_feature_block_addresses(
    block_store: &BlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    series: &mut BTreeMap<u64, BlockAddress>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    let unique_addresses = unique_feature_block_addresses(series);
    let mut rewritten = HashMap::<BlockAddress, BlockAddress>::new();
    for old_address in unique_addresses {
        if !rewrite_stats.should_relocate(&old_address) {
            continue;
        }
        let cold_block = !block_memory_resident(cache, shard_id, &old_address);
        let bytes = read_block_bytes_for_compaction(cache, block_store, shard_id, &old_address)
            .ok_or_else(|| {
                Status::error(
                    "page_compaction_failed",
                    "missing feature page bytes during compaction",
                )
            })?;
        let new_address = block_store
            .append_block_of_object(
                &bytes,
                old_address.object_id(),
                old_address.routing_bucket(),
                old_address.block_id().unwrap_or_default() as u32,
            )
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.block_slab_id,
                new_address.offset,
                new_address.length,
                new_address.routing_bucket(),
                new_address.generation(),
            ),
            bytes,
        );
        rewritten.insert(old_address, new_address);
        rewrite_stats.record(model_id, cold_block);
    }
    for address in series.values_mut() {
        if let Some(new_address) = rewritten.get(address) {
            *address = new_address.clone();
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// THE RELOCATION HINT: which pages a round would move, and WHY, asked one object at a time.
// ---------------------------------------------------------------------------------------------

/// The slabs a relocation round could actually empty.
///
/// A reclaim candidate is any slab carrying dead space, and that is TWO different situations the
/// maintenance round has never told apart:
///
///   - a slab with dead space that objects STILL HOLD BLOCKS ON. Only relocation empties it, so
///     it is compaction's job.
///   - a slab holding nothing but dead space. No object has a page left on it, so there is
///     nothing for compaction to relocate; destroying it is the COLLECTOR's job.
///
/// The second is the one a compaction round manufactures FOR ITSELF. A round relocates a slab's
/// live pages onto a fresh one, and the slab it just emptied stays a reclaim candidate until the
/// collector destroys it. `stale_block_pressure` counts candidates without asking which kind they
/// are, so the round that emptied a slab is the reason the next round runs -- on a shard nobody
/// is writing to, for ever, each round persisting another index record.
///
/// No threshold here, and no shard-wide question about staleness or density: the three predicates
/// of that shape were each refused by the suite. This is the drain SET, and what makes a slab a
/// member of it is that some object still has a page there.
pub fn compaction_drain_block_slab_ids(
    reclaim_candidates: &[StorageReclaimCandidate],
) -> BTreeSet<u64> {
    reclaim_candidates
        .iter()
        .filter(|candidate| candidate.live_block_refs > 0)
        .map(|candidate| candidate.block_slab_id)
        .collect()
}

/// What a relocation round should move FOR THIS OBJECT, as indexes into `object_blocks`.
///
/// The decision belongs at this granularity and not to the shard. An object's pages are worth
/// relocating when they sit on a slab the collector wants emptied, because vacating that slab is
/// the only thing a relocation achieves for them: `compact_block_addresses` copies a page's bytes
/// verbatim and appends them elsewhere, so a page that moves off a slab nobody is draining comes
/// out byte for byte what it went in as, on a slab that is now the one carrying dead space.
///
/// `model_id` is the second half of the question and has no answer to give yet IN THIS TREE, for
/// the reason just given -- every model's pages are copied verbatim, so no model can improve
/// itself by being rewritten in place. A model that PACKED several components into one page on
/// rewrite could, and this is where it says so: it would name its own pages here whether or not
/// their slab is being drained. The hint carries a per-model tally so the answer stays
/// attributable when that arrives.
pub(super) fn compaction_object_block_hint(
    model_id: &str,
    object_blocks: &[BlockAddress],
    drain_block_slab_ids: &BTreeSet<u64>,
) -> Vec<usize> {
    let _ = model_id;
    object_blocks
        .iter()
        .enumerate()
        .filter(|(_, address)| drain_block_slab_ids.contains(&address.block_slab_id))
        .map(|(index, _)| index)
        .collect()
}

/// The shard's answer, composed from the per-object ones. Walks the live page set once.
///
/// This is the NORMATIVE definition of the hint. The maintenance round does not call it -- it
/// takes the same number off the reclaim plan it has already built, which costs no walk at all --
/// and `the_relocation_hint_agrees_with_the_plan_it_is_taken_from` is what holds the two to the
/// same answer, so a change that separates them fails there rather than in a shipped round.
pub(super) fn compaction_relocation_hint_per_object(
    shard_id: ShardId,
    shard: &ShardState,
    reclaim_candidates: &[StorageReclaimCandidate],
) -> ShardCompactionRelocationHint {
    let drain_block_slab_ids = compaction_drain_block_slab_ids(reclaim_candidates);
    let mut object_blocks: BTreeMap<(String, String), Vec<BlockAddress>> = BTreeMap::new();
    for entry in collect_live_block_entries(shard) {
        object_blocks
            .entry((entry.kind.to_string(), entry.object_key.to_string()))
            .or_default()
            .push(entry.address);
    }
    let examined_object_count = object_blocks.len() as u64;
    let mut relocatable_object_count = 0_u64;
    let mut relocatable_block_refs = 0_u64;
    let mut by_model: BTreeMap<String, u64> = BTreeMap::new();
    for ((model_id, _object_key), pages) in &object_blocks {
        let hint = compaction_object_block_hint(model_id, pages, &drain_block_slab_ids);
        if hint.is_empty() {
            continue;
        }
        relocatable_object_count = relocatable_object_count.saturating_add(1);
        relocatable_block_refs = relocatable_block_refs.saturating_add(hint.len() as u64);
        *by_model.entry(model_id.clone()).or_default() += hint.len() as u64;
    }
    let collector_only_block_slab_ids = reclaim_candidates
        .iter()
        .filter(|candidate| candidate.live_block_refs == 0)
        .map(|candidate| candidate.block_slab_id)
        .collect::<Vec<_>>();
    ShardCompactionRelocationHint {
        shard_id,
        examined_object_count,
        relocatable_object_count,
        relocatable_block_refs,
        drain_block_slab_ids: drain_block_slab_ids.into_iter().collect(),
        collector_only_block_slab_ids,
        relocatable_block_refs_by_model: by_model,
    }
}

/// The same count, off the reclaim plan the maintenance round has already built.
///
/// `live_page_refs` on a candidate is the tally of live page addresses that landed on that slab,
/// summed over the same live page set `compaction_relocation_hint_per_object` walks -- so summing
/// it over the drain set is the per-object answer, aggregated, for no extra walk of the shard.
/// The round runs eight of those already; a ninth to ask whether it should run is not a trade
/// worth making on a loop that ticks every thirty seconds per shard.
pub fn compaction_relocatable_block_refs(reclaim_candidates: &[StorageReclaimCandidate]) -> u64 {
    reclaim_candidates
        .iter()
        .filter(|candidate| candidate.live_block_refs > 0)
        .map(|candidate| candidate.live_block_refs)
        .fold(0_u64, |total, refs| total.saturating_add(refs))
}
