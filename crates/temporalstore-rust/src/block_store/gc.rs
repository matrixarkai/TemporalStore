//! LocalBlockStore garbage-collection methods, extracted from block_store.rs.

use super::*;

impl LocalBlockStore {
    pub fn gc_segments_before(
        &self,
        retain_from_page_segment_id: u64,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs(retain_from_page_segment_id, std::iter::empty())
    }

    pub fn gc_segments_before_with_live_refs(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs_mode(
            retain_from_page_segment_id,
            live_page_segment_ids,
            false,
        )
    }

    pub fn gc_segments_before_with_live_refs_delayed_destroy(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs_mode(
            retain_from_page_segment_id,
            live_page_segment_ids,
            true,
        )
    }

    pub fn gc_segments_before_with_live_refs_utility(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        max_destroy_segments: usize,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        if max_destroy_segments == 0 {
            return self.gc_segments_before_with_live_refs_selected(
                retain_from_page_segment_id,
                live_page_segment_ids,
                delayed_destroy,
                Some(BTreeSet::new()),
            );
        }
        self.gc_segments_before_with_live_refs_policy(
            retain_from_page_segment_id,
            live_page_segment_ids,
            BlockStoreGcPolicy::max_segments(max_destroy_segments),
            delayed_destroy,
        )
    }

    pub fn gc_segments_before_with_live_refs_policy(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        policy: BlockStoreGcPolicy,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let live_page_segment_ids = live_page_segment_ids.into_iter().collect::<BTreeSet<_>>();
        let selected = self
            .gc_policy_plan(
                retain_from_page_segment_id,
                live_page_segment_ids.iter().copied(),
                &policy,
            )?
            .selected_page_segment_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        self.gc_segments_before_with_live_refs_selected(
            retain_from_page_segment_id,
            live_page_segment_ids,
            delayed_destroy,
            Some(selected),
        )
    }

    pub fn gc_policy_plan(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        policy: &BlockStoreGcPolicy,
    ) -> Result<BlockStoreGcPolicyPlan, BlockStoreError> {
        let candidates =
            self.gc_utility_candidates(retain_from_page_segment_id, live_page_segment_ids)?;
        let mut selected_page_segment_ids = Vec::new();
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
            if !utility_allowed || !age_allowed {
                skipped_by_policy_count += 1;
                skipped_by_policy_physical_bytes =
                    skipped_by_policy_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }

            if policy.max_destroy_segments > 0
                && selected_page_segment_ids.len() >= policy.max_destroy_segments
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

            selected_page_segment_ids.push(candidate.page_segment_id);
            selected_physical_bytes = selected_physical_bytes.saturating_add(candidate.bytes);
        }

        Ok(BlockStoreGcPolicyPlan {
            retain_from_page_segment_id,
            selected_page_segment_ids,
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
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<BlockStoreGcUtilityCandidate>, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let current_page_segment_id = inner.page_segment_id;
        let live_page_segment_ids = live_page_segment_ids.into_iter().collect::<BTreeSet<_>>();
        let segment_ids = segment_ids_at(&inner.root)?;
        let mut zone_total_bytes = BTreeMap::<u64, u64>::new();
        let mut zone_used_bytes = BTreeMap::<u64, u64>::new();
        for page_segment_id in &segment_ids {
            let bytes = segment_path(&inner.root, *page_segment_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let zone_id = inner
                .bands
                .get(page_segment_id)
                .map(|band| band.band_id)
                .unwrap_or_else(|| band_id_for_segment(*page_segment_id));
            *zone_total_bytes.entry(zone_id).or_default() = zone_total_bytes
                .get(&zone_id)
                .copied()
                .unwrap_or_default()
                .saturating_add(bytes);
            let below_retention_floor = *page_segment_id < retain_from_page_segment_id;
            let is_current = *page_segment_id == current_page_segment_id;
            let is_live = live_page_segment_ids.contains(page_segment_id);
            if !below_retention_floor || is_current || is_live {
                *zone_used_bytes.entry(zone_id).or_default() = zone_used_bytes
                    .get(&zone_id)
                    .copied()
                    .unwrap_or_default()
                    .saturating_add(bytes);
            }
        }
        let mut candidates = Vec::new();
        let now = now_unix_ms();
        for page_segment_id in segment_ids {
            let below_retention_floor = page_segment_id < retain_from_page_segment_id;
            let is_current = page_segment_id == current_page_segment_id;
            let is_live = live_page_segment_ids.contains(&page_segment_id);
            if below_retention_floor && !is_current && !is_live {
                let bytes = segment_path(&inner.root, page_segment_id)
                    .metadata()
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                let band = inner.bands.get(&page_segment_id);
                let created_unix_ms = band.and_then(|band| band.created_unix_ms);
                let updated_unix_ms = band.and_then(|band| band.updated_unix_ms);
                let age_ms = updated_unix_ms
                    .or(created_unix_ms)
                    .map(|timestamp| now.saturating_sub(timestamp));
                let zone_id = band
                    .map(|band| band.band_id)
                    .unwrap_or_else(|| band_id_for_segment(page_segment_id));
                let total_bytes = zone_total_bytes.get(&zone_id).copied().unwrap_or(bytes);
                let used_bytes = zone_used_bytes.get(&zone_id).copied().unwrap_or_default();
                let stale_bytes = total_bytes.saturating_sub(used_bytes);
                let utility_basis_points = if total_bytes == 0 {
                    0
                } else {
                    used_bytes.saturating_mul(10_000) / total_bytes
                };
                candidates.push(BlockStoreGcUtilityCandidate {
                    page_segment_id,
                    bytes,
                    total_bytes,
                    used_bytes,
                    stale_bytes,
                    utility_basis_points,
                    utility_score: page_segment_utility_score(
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
            left.utility_score
                .cmp(&right.utility_score)
                .then_with(|| right.bytes.cmp(&left.bytes))
                .then_with(|| {
                    right
                        .age_ms
                        .unwrap_or_default()
                        .cmp(&left.age_ms.unwrap_or_default())
                })
                .then_with(|| left.page_segment_id.cmp(&right.page_segment_id))
        });
        Ok(candidates)
    }

    pub(super) fn gc_segments_before_with_live_refs_mode(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs_selected(
            retain_from_page_segment_id,
            live_page_segment_ids,
            delayed_destroy,
            None,
        )
    }

    pub(super) fn gc_segments_before_with_live_refs_selected(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        delayed_destroy: bool,
        selected_page_segment_ids: Option<BTreeSet<u64>>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        if delayed_destroy {
            fs::create_dir_all(delayed_destroy_dir(&inner.root))?;
        }
        let current_page_segment_id = inner.page_segment_id;
        let live_page_segment_ids = live_page_segment_ids.into_iter().collect::<BTreeSet<_>>();
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
        for page_segment_id in segment_ids_at(&inner.root)? {
            let segment_physical_bytes = segment_path(&inner.root, page_segment_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let below_retention_floor = page_segment_id < retain_from_page_segment_id;
            let is_current = page_segment_id == current_page_segment_id;
            let is_live = live_page_segment_ids.contains(&page_segment_id);
            let is_selected = selected_page_segment_ids
                .as_ref()
                .map(|selected| selected.contains(&page_segment_id))
                .unwrap_or(true);
            if below_retention_floor && !is_current && !is_live && is_selected {
                removed_physical_bytes += segment_physical_bytes;
                if delayed_destroy {
                    move_segment_to_delayed_destroy(&inner.root, page_segment_id)?;
                    set_band_state(
                        &mut inner.bands,
                        page_segment_id,
                        BlockStoreBandState::DelayedDestroy,
                    );
                    delayed_destroy_ids.push(page_segment_id);
                    delayed_destroy_physical_bytes += segment_physical_bytes;
                } else {
                    fs::remove_file(segment_path(&inner.root, page_segment_id))?;
                    set_band_state(
                        &mut inner.bands,
                        page_segment_id,
                        BlockStoreBandState::Purged,
                    );
                }
                removed.push(page_segment_id);
            } else {
                if below_retention_floor && is_current {
                    retained_current.push(page_segment_id);
                    retained_current_physical_bytes += segment_physical_bytes;
                }
                if below_retention_floor && is_live {
                    retained_live.push(page_segment_id);
                    retained_live_physical_bytes += segment_physical_bytes;
                }
                retained_physical_bytes += segment_physical_bytes;
                retained.push(page_segment_id);
            }
        }
        persist_band_manifest(&inner.root, &inner.bands)?;
        Ok(BlockStoreGcReport {
            retain_from_page_segment_id,
            removed_page_segment_ids: removed,
            retained_page_segment_ids: retained,
            removed_physical_bytes,
            retained_physical_bytes,
            delayed_destroy_page_segment_ids: delayed_destroy_ids,
            delayed_destroy_physical_bytes,
            retained_live_page_segment_ids: retained_live,
            retained_live_physical_bytes,
            retained_current_page_segment_ids: retained_current,
            retained_current_physical_bytes,
        })
    }
}
