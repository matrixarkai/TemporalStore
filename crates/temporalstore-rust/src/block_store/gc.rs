// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalBlockStore garbage-collection methods, extracted from block_store.rs.

use super::*;

impl LocalBlockStore {
    pub fn gc_slabs_before(
        &self,
        retain_from_block_slab_id: u64,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_slabs_before_with_live_refs(retain_from_block_slab_id, std::iter::empty())
    }

    pub fn gc_slabs_before_with_live_refs(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_slabs_before_with_live_refs_mode(
            retain_from_block_slab_id,
            live_block_slab_ids,
            false,
        )
    }

    pub fn gc_slabs_before_with_live_refs_delayed_destroy(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_slabs_before_with_live_refs_mode(
            retain_from_block_slab_id,
            live_block_slab_ids,
            true,
        )
    }

    pub fn gc_slabs_before_with_live_refs_utility(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        max_destroy_slabs: usize,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        if max_destroy_slabs == 0 {
            return self.gc_slabs_before_with_live_refs_selected(
                retain_from_block_slab_id,
                live_block_slab_ids,
                delayed_destroy,
                Some(BTreeSet::new()),
            );
        }
        self.gc_slabs_before_with_live_refs_policy(
            retain_from_block_slab_id,
            live_block_slab_ids,
            BlockStoreGcPolicy::max_slabs(max_destroy_slabs),
            delayed_destroy,
        )
    }

    pub fn gc_slabs_before_with_live_refs_policy(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        policy: BlockStoreGcPolicy,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let selected = self
            .gc_policy_plan(
                retain_from_block_slab_id,
                live_block_slab_ids.iter().copied(),
                &policy,
            )?
            .selected_block_slab_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        self.gc_slabs_before_with_live_refs_selected(
            retain_from_block_slab_id,
            live_block_slab_ids,
            delayed_destroy,
            Some(selected),
        )
    }

    pub fn gc_policy_plan(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        policy: &BlockStoreGcPolicy,
    ) -> Result<BlockStoreGcPolicyPlan, BlockStoreError> {
        let candidates =
            self.gc_utility_candidates(retain_from_block_slab_id, live_block_slab_ids)?;
        let mut selected_block_slab_ids = Vec::new();
        let mut selected_physical_bytes = 0_u64;
        let candidate_physical_bytes = candidates.iter().map(|candidate| candidate.bytes).sum();
        let candidate_total_bytes = candidates
            .iter()
            .map(|candidate| candidate.total_bytes)
            .sum::<u64>();
        let candidate_used_bytes = candidates
            .iter()
            .map(|candidate| candidate.used_bytes)
            .sum::<u64>();
        let candidate_stale_bytes = candidates
            .iter()
            .map(|candidate| candidate.stale_bytes)
            .sum::<u64>();
        let candidate_utility_basis_points = if candidate_total_bytes == 0 {
            0
        } else {
            candidate_used_bytes.saturating_mul(10_000) / candidate_total_bytes
        };
        let mut skipped_by_policy_count = 0_usize;
        let mut skipped_by_policy_physical_bytes = 0_u64;
        let mut skipped_by_budget_count = 0_usize;
        let mut skipped_by_budget_physical_bytes = 0_u64;

        for candidate in &candidates {
            let utility_allowed = policy
                .max_utility_score
                .map(|max_score| candidate.utility_score <= max_score)
                .unwrap_or(true);
            let age_allowed = policy
                .min_age_ms
                .map(|min_age| candidate.age_ms.unwrap_or_default() >= min_age)
                .unwrap_or(true);
            // Garbage-ratio gate (GetGarbageRate threshold): keep bands whose
            // garbage ratio is below the floor. garbage = 10_000 - live-fraction.
            let garbage_allowed = policy
                .min_slab_garbage_basis_points
                .map(|floor| 10_000u64.saturating_sub(candidate.utility_basis_points) >= floor)
                .unwrap_or(true);
            if !utility_allowed || !age_allowed || !garbage_allowed {
                skipped_by_policy_count += 1;
                skipped_by_policy_physical_bytes =
                    skipped_by_policy_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }

            if policy.max_destroy_slabs > 0
                && selected_block_slab_ids.len() >= policy.max_destroy_slabs
            {
                skipped_by_budget_count += 1;
                skipped_by_budget_physical_bytes =
                    skipped_by_budget_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }
            if policy.max_destroy_physical_bytes > 0
                && selected_physical_bytes.saturating_add(candidate.bytes)
                    > policy.max_destroy_physical_bytes
            {
                skipped_by_budget_count += 1;
                skipped_by_budget_physical_bytes =
                    skipped_by_budget_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }

            selected_block_slab_ids.push(candidate.block_slab_id);
            selected_physical_bytes = selected_physical_bytes.saturating_add(candidate.bytes);
        }

        Ok(BlockStoreGcPolicyPlan {
            retain_from_block_slab_id,
            selected_block_slab_ids,
            selected_physical_bytes,
            candidate_total_bytes,
            candidate_used_bytes,
            candidate_stale_bytes,
            candidate_utility_basis_points,
            candidate_count: candidates.len(),
            candidate_physical_bytes,
            skipped_by_policy_count,
            skipped_by_policy_physical_bytes,
            skipped_by_budget_count,
            skipped_by_budget_physical_bytes,
            candidates,
        })
    }

    pub fn gc_utility_candidates(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<BlockStoreGcUtilityCandidate>, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let current_block_slab_id = inner.block_slab_id;
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let slab_ids = slab_ids_at(&inner.root)?;
        let mut band_total_bytes = BTreeMap::<u64, u64>::new();
        let mut band_used_bytes = BTreeMap::<u64, u64>::new();
        for block_slab_id in &slab_ids {
            let bytes = slab_path(&inner.root, *block_slab_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let band_id = inner
                .bands
                .get(block_slab_id)
                .map(|band| band.band_id)
                .unwrap_or_else(|| band_id_for_slab(*block_slab_id));
            *band_total_bytes.entry(band_id).or_default() = band_total_bytes
                .get(&band_id)
                .copied()
                .unwrap_or_default()
                .saturating_add(bytes);
            let below_retention_floor = *block_slab_id < retain_from_block_slab_id;
            let is_current = *block_slab_id == current_block_slab_id;
            let is_live = live_block_slab_ids.contains(block_slab_id);
            if !below_retention_floor || is_current || is_live {
                *band_used_bytes.entry(band_id).or_default() = band_used_bytes
                    .get(&band_id)
                    .copied()
                    .unwrap_or_default()
                    .saturating_add(bytes);
            }
        }
        let mut candidates = Vec::new();
        let now = now_unix_ms();
        for block_slab_id in slab_ids {
            let below_retention_floor = block_slab_id < retain_from_block_slab_id;
            let is_current = block_slab_id == current_block_slab_id;
            let is_live = live_block_slab_ids.contains(&block_slab_id);
            if below_retention_floor && !is_current && !is_live {
                let bytes = slab_path(&inner.root, block_slab_id)
                    .metadata()
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                let band = inner.bands.get(&block_slab_id);
                let created_unix_ms = band.and_then(|band| band.created_unix_ms);
                let updated_unix_ms = band.and_then(|band| band.updated_unix_ms);
                let age_ms = updated_unix_ms
                    .or(created_unix_ms)
                    .map(|timestamp| now.saturating_sub(timestamp));
                let band_id = band
                    .map(|band| band.band_id)
                    .unwrap_or_else(|| band_id_for_slab(block_slab_id));
                let total_bytes = band_total_bytes.get(&band_id).copied().unwrap_or(bytes);
                let used_bytes = band_used_bytes.get(&band_id).copied().unwrap_or_default();
                let stale_bytes = total_bytes.saturating_sub(used_bytes);
                let utility_basis_points = if total_bytes == 0 {
                    0
                } else {
                    used_bytes.saturating_mul(10_000) / total_bytes
                };
                candidates.push(BlockStoreGcUtilityCandidate {
                    block_slab_id,
                    bytes,
                    total_bytes,
                    used_bytes,
                    stale_bytes,
                    utility_basis_points,
                    utility_score: block_slab_utility_score(
                        below_retention_floor,
                        is_current,
                        is_live,
                    ),
                    created_unix_ms,
                    updated_unix_ms,
                    age_ms,
                });
            }
        }
        candidates.sort_by(|left, right| {
            // Reclaim the highest-garbage band first: a lower band live-fraction
            // (utility_basis_points) means more garbage, so ascending live-fraction ==
            // descending garbage ratio. That is the GC victim order, which the previous
            // key (a categorical utility_score, uniformly 0 for all candidates) never
            // actually applied.
            left.utility_basis_points
                .cmp(&right.utility_basis_points)
                .then_with(|| left.utility_score.cmp(&right.utility_score))
                .then_with(|| right.bytes.cmp(&left.bytes))
                .then_with(|| {
                    right
                        .age_ms
                        .unwrap_or_default()
                        .cmp(&left.age_ms.unwrap_or_default())
                })
                .then_with(|| left.block_slab_id.cmp(&right.block_slab_id))
        });
        Ok(candidates)
    }

    pub(super) fn gc_slabs_before_with_live_refs_mode(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_slabs_before_with_live_refs_selected(
            retain_from_block_slab_id,
            live_block_slab_ids,
            delayed_destroy,
            None,
        )
    }

    pub(super) fn gc_slabs_before_with_live_refs_selected(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        delayed_destroy: bool,
        selected_block_slab_ids: Option<BTreeSet<u64>>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        if delayed_destroy {
            fs::create_dir_all(delayed_destroy_dir(&inner.root))?;
        }
        let current_block_slab_id = inner.block_slab_id;
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let mut removed = Vec::new();
        let mut retained = Vec::new();
        let mut delayed_destroy_ids = Vec::new();
        let mut retained_live = Vec::new();
        let mut retained_current = Vec::new();
        let mut removed_physical_bytes = 0;
        let mut retained_physical_bytes = 0;
        let mut delayed_destroy_physical_bytes = 0;
        let mut retained_live_physical_bytes = 0;
        let mut retained_current_physical_bytes = 0;
        for block_slab_id in slab_ids_at(&inner.root)? {
            let slab_physical_bytes = slab_path(&inner.root, block_slab_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let below_retention_floor = block_slab_id < retain_from_block_slab_id;
            let is_current = block_slab_id == current_block_slab_id;
            let is_live = live_block_slab_ids.contains(&block_slab_id);
            let is_selected = selected_block_slab_ids
                .as_ref()
                .map(|selected| selected.contains(&block_slab_id))
                .unwrap_or(true);
            if below_retention_floor && !is_current && !is_live && is_selected {
                removed_physical_bytes += slab_physical_bytes;
                if delayed_destroy {
                    move_slab_to_delayed_destroy(&inner.root, block_slab_id)?;
                    set_band_state(
                        &mut inner.bands,
                        block_slab_id,
                        BlockStoreSlabState::DelayedDestroy,
                    );
                    delayed_destroy_ids.push(block_slab_id);
                    delayed_destroy_physical_bytes += slab_physical_bytes;
                } else {
                    fs::remove_file(slab_path(&inner.root, block_slab_id))?;
                    set_band_state(
                        &mut inner.bands,
                        block_slab_id,
                        BlockStoreSlabState::Purged,
                    );
                }
                removed.push(block_slab_id);
            } else {
                if below_retention_floor && is_current {
                    retained_current.push(block_slab_id);
                    retained_current_physical_bytes += slab_physical_bytes;
                }
                if below_retention_floor && is_live {
                    retained_live.push(block_slab_id);
                    retained_live_physical_bytes += slab_physical_bytes;
                }
                retained_physical_bytes += slab_physical_bytes;
                retained.push(block_slab_id);
            }
        }
        persist_band_manifest(&inner.root, &inner.bands)?;
        Ok(BlockStoreGcReport {
            retain_from_block_slab_id,
            removed_block_slab_ids: removed,
            retained_block_slab_ids: retained,
            removed_physical_bytes,
            retained_physical_bytes,
            delayed_destroy_block_slab_ids: delayed_destroy_ids,
            delayed_destroy_physical_bytes,
            retained_live_block_slab_ids: retained_live,
            retained_live_physical_bytes,
            retained_current_block_slab_ids: retained_current,
            retained_current_physical_bytes,
        })
    }
}
