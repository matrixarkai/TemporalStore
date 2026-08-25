// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction utility/policy/layout reporting helpers, split from engine.rs.
use super::*;

pub(super) fn compaction_utility_report(
    page_store: &LocalBlockStore,
    shard: &ShardState,
) -> ShardCompactionUtilityReport {
    let entries = collect_live_page_entries(shard);
    let addresses = entries
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| entry.address.clone())
        .collect::<Vec<_>>();
    let live_page_slab_ids = addresses
        .iter()
        .map(|address| address.page_slab_id)
        .collect::<BTreeSet<_>>();
    let slab_page_counts = page_store
        .slab_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_slab_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
    let total_page_count = live_page_slab_ids
        .iter()
        .map(|page_slab_id| {
            slab_page_counts
                .get(page_slab_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let live_page_refs = addresses.len() as u64;
    let stale_page_estimate = total_page_count.saturating_sub(live_page_refs);
    let live_ref_density_basis_points = if total_page_count == 0 {
        0
    } else {
        live_page_refs.saturating_mul(10_000) / total_page_count
    };
    ShardCompactionUtilityReport {
        live_page_slab_count: live_page_slab_ids.len(),
        total_page_count,
        live_page_refs,
        stale_page_estimate,
        live_ref_density_basis_points,
        model_policies: model_compaction_policy_reports(shard, &entries, &slab_page_counts),
    }
}

pub(super) fn model_compaction_policy_reports(
    shard: &ShardState,
    entries: &[LivePageEntry],
    slab_page_counts: &BTreeMap<u64, u64>,
) -> Vec<ModelCompactionPolicyReport> {
    #[derive(Default)]
    struct ModelStats {
        live_page_refs: u64,
        deleted_page_refs: u64,
        slab_ids: BTreeSet<u64>,
    }

    let mut by_model = BTreeMap::<String, ModelStats>::new();
    for entry in entries {
        let stats = by_model.entry(entry.kind.clone()).or_default();
        if entry.deleted {
            stats.deleted_page_refs = stats.deleted_page_refs.saturating_add(1);
        } else {
            stats.live_page_refs = stats.live_page_refs.saturating_add(1);
            stats.slab_ids.insert(entry.address.page_slab_id);
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
        } else if shard.control_state_pages.contains_key(key) {
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
        stats.deleted_page_refs = stats.deleted_page_refs.saturating_add(1);
    }

    by_model
        .into_iter()
        .map(|(model_id, stats)| {
            let total_slab_pages = stats
                .slab_ids
                .iter()
                .map(|slab_id| {
                    slab_page_counts
                        .get(slab_id)
                        .copied()
                        .unwrap_or_default()
                })
                .sum::<u64>();
            let stale_page_estimate = total_slab_pages.saturating_sub(stats.live_page_refs);
            let stale_density_basis_points = if total_slab_pages == 0 {
                0
            } else {
                stale_page_estimate.saturating_mul(10_000) / total_slab_pages
            };
            let total_refs = stats.live_page_refs.saturating_add(stats.deleted_page_refs);
            let tombstone_density_basis_points = if total_refs == 0 {
                0
            } else {
                stats.deleted_page_refs.saturating_mul(10_000) / total_refs
            };
            let layout_policy = compaction_layout_policy_for_model(&model_id);
            let stale_density_triggered = stale_density_basis_points > 0;
            let tombstone_compaction_triggered =
                stats.deleted_page_refs > 0 || tombstone_density_basis_points > 0;
            let object_page_packing_enabled = compaction_object_page_packing_enabled(&model_id);
            let layout_aware_rewrite_required = object_page_packing_enabled
                || matches!(
                    layout_policy,
                    "timestamped_chunked_pages" | "context_timeline_or_sidecar_pages"
                )
                || model_id == "control_state";
            ModelCompactionPolicyReport {
                layout_policy: layout_policy.to_string(),
                object_page_packing_enabled,
                model_id,
                live_page_refs: stats.live_page_refs,
                deleted_page_refs: stats.deleted_page_refs,
                total_slab_pages,
                stale_page_estimate,
                stale_density_basis_points,
                tombstone_density_basis_points,
                object_page_pack_group_count: stats.slab_ids.len() as u64,
                cold_page_rewrite_eligible_refs: stats.live_page_refs,
                compaction_action: compaction_action_for_policy(
                    stats.live_page_refs,
                    stats.deleted_page_refs,
                    stale_density_basis_points,
                    tombstone_density_basis_points,
                )
                .to_string(),
                stale_density_triggered,
                tombstone_compaction_triggered,
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

pub(super) fn compaction_object_page_packing_enabled(model_id: &str) -> bool {
    matches!(
        compaction_layout_policy_for_model(model_id),
        "single_page_object" | "component_page_object"
    )
}

pub(super) fn compaction_action_for_policy(
    live_page_refs: u64,
    deleted_page_refs: u64,
    stale_density_basis_points: u64,
    tombstone_density_basis_points: u64,
) -> &'static str {
    if live_page_refs == 0 && deleted_page_refs > 0 {
        "drop_tombstones"
    } else if tombstone_density_basis_points > 0 || deleted_page_refs > 0 {
        "rewrite_live_drop_tombstones"
    } else if stale_density_basis_points > 0 {
        "rewrite_stale_density"
    } else {
        "rewrite_cold_or_pack"
    }
}

#[derive(Debug, Default)]
pub(super) struct CompactionRewriteStats {
    pub(super) rewritten_page_refs: usize,
    pub(super) cold_page_rewrite_refs: usize,
    by_model: BTreeMap<String, ModelCompactionRewriteStats>,
}

#[derive(Debug, Default)]
pub(super) struct ModelCompactionRewriteStats {
    rewritten_page_refs: usize,
    cold_page_rewrite_refs: usize,
}

impl CompactionRewriteStats {
    fn record(&mut self, model_id: &str, cold_page: bool) {
        self.rewritten_page_refs = self.rewritten_page_refs.saturating_add(1);
        let model = self.by_model.entry(model_id.to_string()).or_default();
        model.rewritten_page_refs = model.rewritten_page_refs.saturating_add(1);
        if cold_page {
            self.cold_page_rewrite_refs = self.cold_page_rewrite_refs.saturating_add(1);
            model.cold_page_rewrite_refs = model.cold_page_rewrite_refs.saturating_add(1);
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
                    rewritten_page_refs: stats.rewritten_page_refs,
                    cold_page_rewrite_refs: stats.cold_page_rewrite_refs,
                    object_page_pack_group_count: before_policy
                        .map(|policy| policy.object_page_pack_group_count as usize)
                        .unwrap_or_default(),
                    tombstone_density_basis_points: before_policy
                        .map(|policy| policy.tombstone_density_basis_points)
                        .unwrap_or_default(),
                    stale_density_basis_points: before_policy
                        .map(|policy| policy.stale_density_basis_points)
                        .unwrap_or_default(),
                }
            })
            .collect()
    }
}

pub(super) fn page_memory_resident(cache: &MultiLayerCache, shard_id: ShardId, address: &BlockAddress) -> bool {
    cache
        .get_memory(&CacheKey::page_with_slot_generation(
            shard_id,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket,
            address.generation,
        ))
        .is_some()
}

pub(super) fn compaction_model_layout_reports(
    page_store: &LocalBlockStore,
    shard: &ShardState,
) -> Vec<ShardCompactionModelLayoutReport> {
    let slab_page_counts = page_store
        .slab_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_slab_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
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
        .map(|address| address.page_slab_id)
        .collect::<BTreeSet<_>>();
    let total_pages_in_live_slabs = live_slab_ids
        .iter()
        .map(|slab_id| {
            slab_page_counts
                .get(slab_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let unique_page_refs = unique_addresses.len();
    let packed_timestamped_pages = packed_pages.unwrap_or_default();
    let live_ref_density_basis_points = if total_pages_in_live_slabs == 0 {
        0
    } else {
        (unique_page_refs as u64).saturating_mul(10_000) / total_pages_in_live_slabs
    };
    ShardCompactionModelLayoutReport {
        kind: kind.to_string(),
        object_count,
        index_refs: addresses.len(),
        unique_page_refs,
        packed_timestamped_pages,
        legacy_value_pages: unique_page_refs.saturating_sub(packed_timestamped_pages),
        stale_page_estimate: total_pages_in_live_slabs.saturating_sub(unique_page_refs as u64),
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

pub(super) fn compact_page_addresses<'a>(
    page_store: &LocalBlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    addresses: impl IntoIterator<Item = &'a mut BlockAddress>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    for address in addresses {
        let cold_page = !page_memory_resident(cache, shard_id, address);
        let bytes = read_page_bytes(cache, page_store, shard_id, address).ok_or_else(|| {
            Status::error(
                "page_compaction_failed",
                "missing page bytes during compaction",
            )
        })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, address.object_id, address.routing_bucket)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.page_slab_id,
                new_address.offset,
                new_address.length,
                new_address.routing_bucket,
                new_address.generation,
            ),
            bytes,
        );
        rewrite_stats.record(model_id, cold_page);
    }
    Ok(())
}

pub(super) fn compact_feature_page_addresses(
    page_store: &LocalBlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    series: &mut BTreeMap<u64, BlockAddress>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    let unique_addresses = unique_feature_page_addresses(series);
    let mut rewritten = HashMap::<BlockAddress, BlockAddress>::new();
    for old_address in unique_addresses {
        let cold_page = !page_memory_resident(cache, shard_id, &old_address);
        let bytes =
            read_page_bytes(cache, page_store, shard_id, &old_address).ok_or_else(|| {
                Status::error(
                    "page_compaction_failed",
                    "missing feature page bytes during compaction",
                )
            })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, old_address.object_id, old_address.routing_bucket)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.page_slab_id,
                new_address.offset,
                new_address.length,
                new_address.routing_bucket,
                new_address.generation,
            ),
            bytes,
        );
        rewritten.insert(old_address, new_address);
        rewrite_stats.record(model_id, cold_page);
    }
    for address in series.values_mut() {
        if let Some(new_address) = rewritten.get(address) {
            *address = new_address.clone();
        }
    }
    Ok(())
}

