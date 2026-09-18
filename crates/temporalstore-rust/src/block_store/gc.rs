// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BlockStore garbage-collection methods, extracted from block_store.rs.

use super::*;

impl BlockStore {
    /// Take the per-slab live tally the INDEX maintains.
    ///
    /// Publishing rather than deriving, because the store has no way to derive it: a page dies
    /// when an index entry stops naming it, and that event never reaches here. The index keeps a
    /// running tally on its own mutation path and hands a snapshot over; this only reads it.
    ///
    /// Idempotent, and the last publish wins. Cheap: one map, once per maintenance round, sized
    /// by SLABS rather than by pages.
    pub fn publish_live_block_bytes(&self, live: BTreeMap<u64, BlockStoreSlabLive>) {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .live_block_bytes = Some(live);
    }

    /// What was last published, or `None` if nothing ever was.
    pub fn published_live_block_bytes(&self) -> Option<BTreeMap<u64, BlockStoreSlabLive>> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .live_block_bytes
            .clone()
    }

    /// How much of each slab is still live, for EVERY slab -- not just the collectable ones.
    ///
    /// This is the answer to "does `utility_basis_points` stop being uniformly zero", and it has
    /// to be asked of every slab to be worth asking. The GC candidate list cannot answer it: a
    /// candidate is a slab that no live page points at, so its live fraction is zero by
    /// construction and will stay zero however the figure is computed. The slabs with an
    /// interesting fraction are exactly the ones the collector is not allowed to touch.
    ///
    /// Empty live figures when nothing has published; the physical and logical columns still
    /// stand, so a caller can tell "no live pages" from "no tally".
    pub fn slab_live_fractions(&self) -> Result<Vec<BlockStoreSlabLiveFraction>, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let published = inner.live_block_bytes.clone().unwrap_or_default();
        let mut out = Vec::new();
        for block_slab_id in slab_ids_at(&inner.root)? {
            let physical_bytes = slab_path(&inner.root, block_slab_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let logical_bytes = inner
                .slabs
                .get(&block_slab_id)
                .map(|slab| slab.logical_bytes)
                .filter(|bytes| *bytes > 0)
                .unwrap_or(physical_bytes);
            let live = published.get(&block_slab_id).copied().unwrap_or_default();
            // Capped at the denominator. A live page can be counted at its logical length while
            // the slab total was written before that page was rewritten in place, and a fraction
            // above 1 is never a useful reading.
            let live_bytes = live.live_bytes.min(logical_bytes);
            let live_basis_points = if logical_bytes == 0 {
                0
            } else {
                live_bytes.saturating_mul(10_000) / logical_bytes
            };
            out.push(BlockStoreSlabLiveFraction {
                block_slab_id,
                physical_bytes,
                logical_bytes,
                live_block_refs: live.live_block_refs,
                live_bytes,
                live_basis_points,
                garbage_basis_points: 10_000_u64.saturating_sub(live_basis_points),
            });
        }
        Ok(out)
    }

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

    /// Policy-planned collection, narrowed to the slabs a caller's own analysis still allows.
    ///
    /// Two independent filters, deliberately kept separate. The POLICY decides which candidates
    /// are worth collecting (garbage ratio, age, per-round budget); `allowed_block_slab_ids`
    /// carries a decision the block store cannot make for itself -- which slabs are pinned by a
    /// follower's replay cursor, a snapshot floor or a retained manifest. Intersecting them here
    /// rather than folding one into the other keeps either side auditable on its own: the plan
    /// still reports what the policy skipped, and what the caller withheld does not masquerade
    /// as a policy decision.
    ///
    /// `None` allows everything the policy selected, which is what the unnarrowed entry does.
    pub fn gc_slabs_before_with_live_refs_policy_limited(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        policy: BlockStoreGcPolicy,
        delayed_destroy: bool,
        allowed_block_slab_ids: Option<BTreeSet<u64>>,
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
            .filter(|block_slab_id| {
                allowed_block_slab_ids
                    .as_ref()
                    .map(|allowed| allowed.contains(block_slab_id))
                    .unwrap_or(true)
            })
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
            // Garbage-ratio gate (GetGarbageRate threshold): keep slabs whose
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
        let published_live = inner.live_block_bytes.clone();
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let slab_ids = slab_ids_at(&inner.root)?;
        let mut slab_total_bytes = BTreeMap::<u64, u64>::new();
        let mut slab_used_bytes = BTreeMap::<u64, u64>::new();
        for block_slab_id in &slab_ids {
            let bytes = slab_path(&inner.root, *block_slab_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let stored_slab_id = inner
                .slabs
                .get(block_slab_id)
                .map(|slab| slab.stored_slab_id)
                .unwrap_or(*block_slab_id);
            *slab_total_bytes.entry(stored_slab_id).or_default() = slab_total_bytes
                .get(&stored_slab_id)
                .copied()
                .unwrap_or_default()
                .saturating_add(bytes);
            let below_retention_floor = *block_slab_id < retain_from_block_slab_id;
            let is_current = *block_slab_id == current_block_slab_id;
            let is_live = live_block_slab_ids.contains(block_slab_id);
            if !below_retention_floor || is_current || is_live {
                *slab_used_bytes.entry(stored_slab_id).or_default() = slab_used_bytes
                    .get(&stored_slab_id)
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
                let slab = inner.slabs.get(&block_slab_id);
                let created_unix_ms = slab.and_then(|slab| slab.created_unix_ms);
                let updated_unix_ms = slab.and_then(|slab| slab.updated_unix_ms);
                let age_ms = updated_unix_ms
                    .or(created_unix_ms)
                    .map(|timestamp| now.saturating_sub(timestamp));
                let stored_slab_id = slab
                    .map(|slab| slab.stored_slab_id)
                    .unwrap_or(block_slab_id);
                // USED BYTES MEANS LIVE PAGE BYTES IN THIS SLAB, once an index has published a
                // tally. Before that it means what it always meant, which is a different
                // quantity and a much worse one: the file sizes of the slabs grouped under the
                // same stored id that are NOT collectable. Since the candidate filter is the
                // exact negation of that test, and a stored id names exactly one slab, a
                // candidate could never contribute to its own used bytes -- so every candidate
                // reported zero, and reported it by accident rather than by measurement.
                //
                // The denominator changes with it. A live page is counted at its LOGICAL length,
                // so the total it is a fraction of has to be logical too: the slab descriptor
                // carries exactly that, `logical_bytes`, summed over every page ever appended
                // here. That field only ever grows -- which is a BUG when it is read as a live
                // figure, and is precisely right for a denominator.
                //
                // The numbers for a CANDIDATE do not move, and that is the point: a candidate is
                // a slab no live page points at, so its maintained live bytes are zero. It now
                // reads zero because it was counted, not because two filters happened to
                // contradict each other. `slab_live_fractions` is where the slabs with a fraction
                // between the two extremes are visible.
                let (total_bytes, used_bytes) = match published_live.as_ref() {
                    Some(published) => {
                        let logical_bytes = slab
                            .map(|slab| slab.logical_bytes)
                            .filter(|logical| *logical > 0)
                            .unwrap_or(bytes);
                        let live_bytes = published
                            .get(&block_slab_id)
                            .map(|live| live.live_bytes)
                            .unwrap_or_default();
                        (logical_bytes, live_bytes.min(logical_bytes))
                    }
                    None => (
                        slab_total_bytes.get(&stored_slab_id).copied().unwrap_or(bytes),
                        slab_used_bytes.get(&stored_slab_id).copied().unwrap_or_default(),
                    ),
                };
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
            // Reclaim the highest-garbage slab first: a lower slab live-fraction
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
        let mut retained_live_bytes = Vec::new();
        let mut removed_physical_bytes = 0;
        let mut retained_physical_bytes = 0;
        let mut delayed_destroy_physical_bytes = 0;
        let mut retained_live_physical_bytes = 0;
        let mut retained_current_physical_bytes = 0;
        let mut retained_live_bytes_physical_bytes = 0;
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
            // ASK THE TALLY ONE LAST TIME, IMMEDIATELY BEFORE THE IRREVERSIBLE STEP.
            //
            // The header on `live_block_bytes` states the rule this enforces: the tally "only ever
            // KEEPS a slab; it never grants permission to delete one". Nothing made that true. The
            // only consumer was the garbage floor in `gc_policy_plan`, and a floor is a THRESHOLD
            // -- at the shipped 4,000 basis points a slab the tally credits with 200 live bytes out
            // of 1,000 is 8,000 bp of garbage, clears the floor, and is selected for destruction.
            // So the tally could keep a slab only once it was more than 60% live, and below that it
            // was silently overruled in exactly the case it exists to notice.
            //
            // The two reclaim entries that take no policy at all -- `gc_slabs_before_with_live_refs`
            // and its delayed-destroy sibling, which is what the operator reclaim RPC calls -- never
            // consulted it in any form.
            //
            // The constructor doc on `with_slab_garbage_floor` states the invariant from the other
            // side: a collector candidate is a slab no live page points at, "so its maintained live
            // bytes are genuinely zero". That was asserted in prose and nowhere in code. It is a
            // claim about two INDEPENDENT derivations agreeing -- `live_block_slab_ids` is walked
            // fresh from the index at every call, while the tally is maintained incrementally on the
            // index's own mutation path and has a drift check of its own -- so the case where they
            // disagree is real, and the direction that matters is the tally saying live where the
            // walk said dead.
            //
            // `None` (nobody has published) reads as zero and the check is inert, which is the only
            // reading that is safe: a store with no tally must reclaim exactly as it did before.
            let tallied_live_bytes = inner
                .live_block_bytes
                .as_ref()
                .and_then(|published| published.get(&block_slab_id))
                .map(|live| live.live_bytes)
                .unwrap_or_default();
            let holds_tallied_live_bytes = tallied_live_bytes > 0;
            if below_retention_floor
                && !is_current
                && !is_live
                && is_selected
                && !holds_tallied_live_bytes
            {
                removed_physical_bytes += slab_physical_bytes;
                if delayed_destroy {
                    move_slab_to_delayed_destroy_unsynced(&inner.root, block_slab_id)?;
                    set_slab_state(
                        &mut inner.slabs,
                        block_slab_id,
                        BlockStoreSlabState::DelayedDestroy,
                    );
                    delayed_destroy_ids.push(block_slab_id);
                    delayed_destroy_physical_bytes += slab_physical_bytes;
                } else {
                    fs::remove_file(slab_path(&inner.root, block_slab_id))?;
                    set_slab_state(
                        &mut inner.slabs,
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
                // Everything the destroy branch required EXCEPT the tally. Reported on its own so
                // a slab held back by the disagreement cannot be read as an ordinary retention.
                if below_retention_floor
                    && !is_current
                    && !is_live
                    && is_selected
                    && holds_tallied_live_bytes
                {
                    retained_live_bytes.push(block_slab_id);
                    retained_live_bytes_physical_bytes += slab_physical_bytes;
                }
                retained_physical_bytes += slab_physical_bytes;
                retained.push(block_slab_id);
            }
        }
        // Only when this round actually changed something.
        //
        // `inner.slabs` is mutated in exactly one place in the loop above -- `set_slab_state`,
        // inside the branch that also pushes onto `removed` -- so an empty `removed` means the
        // manifest would be rewritten with byte-identical content. That rewrite is not free: it
        // serialises every slab, fsyncs the temp file, renames it, and fsyncs the parent
        // directory. TWO fsyncs, on a stage the periodic loop runs whenever page pressure holds.
        //
        // Measured on a fixture where the store settles at three slabs and a round reclaims
        // nothing: 4.0 ms per round before, and the round does no other durable work.
        //
        // TWO DIRECTORY FSYNCS FOR THE WHOLE ROUND, and they must land HERE: after every rename,
        // before the manifest that records them.
        //
        // The loop above used to fsync the store root and the trash directory once per
        // quarantined slab -- the SAME two directories every iteration. Measured with
        // `strace -y -e trace=fsync` on the unmodified loop: quarantining 199 slabs issued 199
        // fsyncs of the trash directory and 199 of the root, and 799 slabs issued 799 and 799.
        // Two per slab, so 160,000 at eighty thousand slabs, to commit one directory entry each.
        // A directory fsync commits every pending entry in that directory, so one pair after the
        // loop makes the same renames durable. See `sync_delayed_destroy_dirs` for why widening
        // the crash window reaches no state the manifest-written-once shape did not already
        // reach.
        if !delayed_destroy_ids.is_empty() {
            sync_delayed_destroy_dirs(&inner.root)?;
        }
        if !removed.is_empty() {
            inner.persist_slab_manifest_counted()?;
        }
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
            retained_live_bytes_block_slab_ids: retained_live_bytes,
            retained_live_bytes_physical_bytes,
        })
    }
}

#[cfg(test)]
#[path = "gc_scale.rs"]
mod gc_scale;
