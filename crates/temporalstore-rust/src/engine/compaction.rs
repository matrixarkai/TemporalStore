//! Compaction utility/policy/layout reporting helpers, split from engine.rs.
use super::*;

pub(super) fn compaction_utility_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> ShardCompactionUtilityReport {
    let entries = collect_live_page_entries(shard);
    let addresses = entries
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| entry.address.clone())
        .collect::<Vec<_>>();
    let live_page_segment_ids = addresses
        .iter()
        .map(|address| address.page_segment_id)
        .collect::<BTreeSet<_>>();
    let segment_page_counts = page_store
        .segment_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_segment_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
    let total_page_count = live_page_segment_ids
        .iter()
        .map(|page_segment_id| {
            segment_page_counts
                .get(page_segment_id)
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
        live_page_segment_count: live_page_segment_ids.len(),
        total_page_count,
        live_page_refs,
        stale_page_estimate,
        live_ref_density_basis_points,
        model_policies: model_compaction_policy_reports(shard, &entries, &segment_page_counts),
    }
}

pub(super) fn model_compaction_policy_reports(
    shard: &ShardState,
    entries: &[LivePageEntry],
    segment_page_counts: &BTreeMap<u64, u64>,
) -> Vec<ModelCompactionPolicyReport> {
    #[derive(Default)]
    struct ModelStats {
        live_page_refs: u64,
        deleted_page_refs: u64,
        segment_ids: BTreeSet<u64>,
    }

    let mut by_model = BTreeMap::<String, ModelStats>::new();
    for entry in entries {
        let stats = by_model.entry(entry.kind.clone()).or_default();
        if entry.deleted {
            stats.deleted_page_refs = stats.deleted_page_refs.saturating_add(1);
        } else {
            stats.live_page_refs = stats.live_page_refs.saturating_add(1);
            stats.segment_ids.insert(entry.address.page_segment_id);
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
        } else if shard.features.contains_key(key) {
            "feature"
        } else if shard.sequences.contains_key(key) {
            "sequence"
        } else if shard.ips.contains_key(key) {
            "ips"
        } else if shard.risk_pages.contains_key(key) {
            "risk"
        } else if shard.context_nodes.contains_key(key) {
            "context_node"
        } else if shard.context_entities.contains_key(key) {
            "context_entity"
        } else if shard.context_embeddings.contains_key(key) {
            "context_embedding"
        } else {
            "string"
        };
        let stats = by_model.entry(model_id.to_string()).or_default();
        stats.deleted_page_refs = stats.deleted_page_refs.saturating_add(1);
    }

    by_model
        .into_iter()
        .map(|(model_id, stats)| {
            let total_segment_pages = stats
                .segment_ids
                .iter()
                .map(|segment_id| {
                    segment_page_counts
                        .get(segment_id)
                        .copied()
                        .unwrap_or_default()
                })
                .sum::<u64>();
            let stale_page_estimate = total_segment_pages.saturating_sub(stats.live_page_refs);
            let stale_density_basis_points = if total_segment_pages == 0 {
                0
            } else {
                stale_page_estimate.saturating_mul(10_000) / total_segment_pages
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
                || model_id == "risk";
            ModelCompactionPolicyReport {
                layout_policy: layout_policy.to_string(),
                object_page_packing_enabled,
                model_id,
                live_page_refs: stats.live_page_refs,
                deleted_page_refs: stats.deleted_page_refs,
                total_segment_pages,
                stale_page_estimate,
                stale_density_basis_points,
                tombstone_density_basis_points,
                object_page_pack_group_count: stats.segment_ids.len() as u64,
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
        "string" | "risk" | "context_node" | "context_entity" | "context_embedding" => {
            "single_page_object"
        }
        "hash" | "set" => "component_page_object",
        "feature" | "sequence" | "ips" => "timestamped_chunked_pages",
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

pub(super) fn page_memory_resident(cache: &MultiLayerCache, shard_id: ShardId, address: &PageAddress) -> bool {
    cache
        .get_memory(&CacheKey::page_with_slot_generation(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
            address.generation,
        ))
        .is_some()
}

pub(super) fn compaction_model_layout_reports(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> Vec<ShardCompactionModelLayoutReport> {
    let segment_page_counts = page_store
        .segment_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_segment_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
    let mut reports = Vec::new();
    reports.push(compaction_layout_from_addresses(
        "string",
        shard.strings.len(),
        shard.strings.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "hash",
        shard.hashes.len(),
        shard
            .hashes
            .values()
            .flat_map(|fields| fields.values().cloned()),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "set",
        shard.sets.len(),
        shard
            .sets
            .values()
            .flat_map(|members| members.values().cloned()),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "feature",
        &shard.features,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "sequence",
        &shard.sequences,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "ips",
        &shard.ips,
        &segment_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_node",
        shard.context_nodes.len(),
        shard.context_nodes.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_event",
        &shard.context_events,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_index",
        &shard.context_indexes,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_audit",
        &shard.context_audits,
        &segment_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_entity",
        shard.context_entities.len(),
        shard.context_entities.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_child",
        &shard.context_children,
        &segment_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_embedding",
        shard.context_embeddings.len(),
        shard.context_embeddings.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_summary",
        &shard.context_summaries,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_compression",
        &shard.context_compressions,
        &segment_page_counts,
    ));
    reports.retain(|report| report.object_count > 0 || report.index_refs > 0);
    reports
}

pub(super) fn compaction_timestamped_layout(
    kind: &str,
    timelines: &HashMap<String, BTreeMap<u64, PageAddress>>,
    segment_page_counts: &BTreeMap<u64, u64>,
) -> ShardCompactionModelLayoutReport {
    let mut ref_counts = HashMap::<PageAddress, usize>::new();
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
        segment_page_counts,
        Some(packed_pages),
    )
    .with_index_refs(ref_counts.values().sum())
}

pub(super) fn compaction_layout_from_addresses(
    kind: &str,
    object_count: usize,
    addresses: impl IntoIterator<Item = PageAddress>,
    segment_page_counts: &BTreeMap<u64, u64>,
    packed_pages: Option<usize>,
) -> ShardCompactionModelLayoutReport {
    let addresses = addresses.into_iter().collect::<Vec<_>>();
    let unique_addresses = addresses
        .iter()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let live_segment_ids = unique_addresses
        .iter()
        .map(|address| address.page_segment_id)
        .collect::<BTreeSet<_>>();
    let total_pages_in_live_segments = live_segment_ids
        .iter()
        .map(|segment_id| {
            segment_page_counts
                .get(segment_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let unique_page_refs = unique_addresses.len();
    let packed_timestamped_pages = packed_pages.unwrap_or_default();
    let live_ref_density_basis_points = if total_pages_in_live_segments == 0 {
        0
    } else {
        (unique_page_refs as u64).saturating_mul(10_000) / total_pages_in_live_segments
    };
    ShardCompactionModelLayoutReport {
        kind: kind.to_string(),
        object_count,
        index_refs: addresses.len(),
        unique_page_refs,
        packed_timestamped_pages,
        legacy_value_pages: unique_page_refs.saturating_sub(packed_timestamped_pages),
        stale_page_estimate: total_pages_in_live_segments.saturating_sub(unique_page_refs as u64),
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
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    addresses: impl IntoIterator<Item = &'a mut PageAddress>,
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
            .append_with_page_metadata(&bytes, address.object_id, address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
                new_address.generation,
            ),
            bytes,
        );
        rewrite_stats.record(model_id, cold_page);
    }
    Ok(())
}

pub(super) fn compact_feature_page_addresses(
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    series: &mut BTreeMap<u64, PageAddress>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    let unique_addresses = unique_feature_page_addresses(series);
    let mut rewritten = HashMap::<PageAddress, PageAddress>::new();
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
            .append_with_page_metadata(&bytes, old_address.object_id, old_address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
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

