// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Index/manifest persistence + load/flush helper methods for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub(super) fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    /// The single funnel through which every consumer reads the COMPLETE served index for
    /// a shard. It always returns a full, current `serialize_index`-shaped byte image of
    /// the `ShardState`, so callers never see a partial or stale index.
    ///
    /// - Shard loaded, not bulk: serialize the LIVE in-memory shard. This is the
    ///   authoritative current state and is what makes deferring the per-write base rewrite
    ///   safe -- readers reconstruct from memory rather than from the on-disk base.
    /// - Otherwise: read the on-disk base `shard-{id}.index.json`. This is the correct source
    ///   in bulk mode, where the base is deliberately frozen at the last flush anchor while
    ///   the WAL tail races ahead (serving the live tail would over-advance the manifest
    ///   replay watermark), and for a shard not currently loaded.
    pub(super) fn load_served_index_bytes(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<u8>, std::io::Error> {
        if !bulk_ingest_mode() {
            if let Some(shard) = self
                .shards
                .read()
                .expect("engine lock poisoned")
                .get(&shard_id)
            {
                return Ok(serialize_index(shard));
            }
        }
        fs::read(self.index_path(shard_id))
    }

    pub(super) fn persist_bucket_dump_manifest(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), std::io::Error> {
        let path =
            bucket_dump_manifest_path(&self.index_dir, manifest.shard_id, &manifest.manifest_id);
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        // The manifest is the durable reclaim watermark (wal_sequence) + recovery
        // index; WAL reclaim durably truncates the WAL based on it. It MUST be written
        // durably and atomically -- a bare fs::write left it in the page cache, so a
        // crash after the (durable) WAL reclaim but before the manifest reached disk
        // lost the records in between (the dumped index must be committed before
        // advancing the dumped-log id; both are waited on together). Atomic temp+rename
        // also prevents a torn manifest from hiding the whole listing on load.
        atomic_write_bytes(&path, &bytes)
    }

    pub(super) fn persist_bucket_dump_install_marker(
        &self,
        manifest: &BucketDumpManifest,
        phase: &str,
    ) -> Result<(), std::io::Error> {
        self.persist_bucket_dump_install_marker_by_fields(
            manifest.shard_id,
            &manifest.manifest_id,
            phase,
            manifest.wal_sequence,
            manifest.index_log_sequence,
        )
    }

    pub(super) fn persist_bucket_dump_install_marker_by_fields(
        &self,
        shard_id: ShardId,
        manifest_id: &str,
        phase: &str,
        wal_sequence: u64,
        index_log_sequence: u64,
    ) -> Result<(), std::io::Error> {
        write_bucket_dump_install_marker(
            &self.index_dir,
            &BucketDumpInstallMarker {
                shard_id,
                manifest_id: manifest_id.to_string(),
                phase: phase.to_string(),
                wal_sequence,
                index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
    }

    pub(super) fn validate_bucket_dump_generation_for_install(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), Status> {
        if manifest.dump_generation_id.is_empty() {
            return Ok(());
        }
        let requested_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
        let source_manifest_ids = manifest
            .source_manifest_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for existing in self.list_bucket_dump_manifests(manifest.shard_id) {
            if existing.manifest_id == manifest.manifest_id
                || source_manifest_ids.contains(&existing.manifest_id)
                || existing.dump_generation_id.is_empty()
                || existing.dump_generation_id == manifest.dump_generation_id
            {
                continue;
            }
            let existing_buckets = existing.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
            let overlaps = requested_buckets.is_empty()
                || existing_buckets.is_empty()
                || !requested_buckets.is_disjoint(&existing_buckets);
            if overlaps
                && existing.index_log_sequence >= manifest.index_log_sequence
                && existing.wal_sequence >= manifest.wal_sequence
            {
                return Err(Status::error(
                    "slot_dump_generation_conflict",
                    format!(
                        "manifest generation {} conflicts with installed generation {} for overlapping slots",
                        manifest.dump_generation_id, existing.dump_generation_id
                    ),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn load_index(&self, shard_id: ShardId, warm_cache: bool) -> Option<ShardState> {
        // Tolerant wrapper for probe/report callers (manifest-anchor probe, recovery reports):
        // a corrupt index-log delta yields None here. The authoritative load path
        // (`load_shard_with`) calls `load_index_checked` instead so it can REFUSE the load on
        // corruption rather than silently serving a base-only prefix.
        self.load_index_inner(shard_id, warm_cache, true)
            .ok()
            .flatten()
    }

    /// Fold-aware load that surfaces a corrupt served-index delta as `Err` so the caller can
    /// refuse the load. Used by the authoritative recovery path.
    pub(super) fn load_index_checked(
        &self,
        shard_id: ShardId,
        warm_cache: bool,
    ) -> Result<Option<ShardState>, Status> {
        self.load_index_inner(shard_id, warm_cache, true)
    }

    /// Load ONLY the durable base snapshot (materialized at the last dump/unload), WITHOUT
    /// folding the served-index delta. Used by single-barrier recovery: the delta-log fdatasync
    /// is deferred, so the delta tail (and the anchor it advances) may reference pages that were
    /// never fsync'd -- trusting it could skip WAL replay and leave dangling references. The base
    /// is a durable checkpoint at its own watermark (flush_shard_index fsyncs every page before
    /// advancing that watermark); the WAL tail beyond it is re-derived by replaying each record
    /// exactly once (no delta fold means no double-apply of non-idempotent commands).
    pub(super) fn load_index_base_only(
        &self,
        shard_id: ShardId,
        warm_cache: bool,
    ) -> Option<ShardState> {
        // base_only never folds the delta log, so this never fails on delta corruption.
        self.load_index_inner(shard_id, warm_cache, false)
            .ok()
            .flatten()
    }

    fn load_index_inner(
        &self,
        shard_id: ShardId,
        warm_cache: bool,
        fold_deltas: bool,
    ) -> Result<Option<ShardState>, Status> {
        let read = fs::read(self.index_path(shard_id));
        let base_present = read.is_ok();
        // The base snapshot is materialized only at compaction/unload, so a crash before the
        // first compaction leaves no base -- start empty and rebuild from the index-log
        // deltas below.
        let mut shard = match read {
            Ok(bytes) => match crate::engine::decode_index_bytes(&bytes) {
                Ok(shard) => {
                    // Refuse an index written in an older on-disk shape. A stale index decodes
                    // CLEANLY -- the types are unchanged -- while a field's meaning has moved
                    // under it, so trusting it serves wrong data with no error anywhere. The
                    // concrete case: before version 2, context_events was keyed by timeline_key;
                    // it is keyed by event_id_hash now, so a v1 index yields timeline keys in the
                    // event-id slot and an empty context_event_timeline, and every time-windowed
                    // read silently returns nothing.
                    //
                    // Refusing is treated exactly like a corrupt or absent index: the caller
                    // falls back to WAL replay, which rebuilds both maps correctly through
                    // insert_context_event_views. Slower, and correct.
                    if shard.index_format_version < super::SHARD_INDEX_FORMAT_VERSION {
                        return Ok(None);
                    }
                    shard
                }
                Err(_) => return Ok(None),
            },
            Err(_) => ShardState::default(),
        };
        // Thin-layer Sequence fold: a pre-fold on-disk index stored Sequence rows in a
        // separate `sequences` map. Sequence now lives in `features` (same timestamped-KV
        // storage, typed row codec at the API layer), so fold any legacy series forward
        // before reconcile rebuilds membership. New indexes never carry `sequences`.
        if !shard.sequences.is_empty() {
            for (key, series) in std::mem::take(&mut shard.sequences) {
                shard.features.entry(key).or_default().extend(series);
            }
        }
        // Fold the index-log deltas beyond the base snapshot's anchor into the bucket index
        // (reconstructing the exact on-disk page layout at the ORIGINAL addresses) and apply
        // the captured per-key non-page state, BEFORE reconcile. This is what lets a crash
        // reload reconstruct the served index WITHOUT re-executing the WAL -- re-execution
        // would write fresh pages and relocate them to the active slab, doubling physical
        // page counts and losing the recorded slab layout.
        if fold_deltas {
            self.fold_index_log_deltas(shard_id, &mut shard)?;
        }
        // No base and nothing to fold -> genuinely nothing persisted yet.
        if !base_present
            && shard.bucket_index.bucket_map.is_empty()
            && shard.applied_wal_sequence.is_none()
        {
            return Ok(None);
        }
        // Fold disk->memory cache promotion into reconcile's page reads on a warming
        // load (normal restart): the pages reconcile reads to rebuild the secondary
        // views are promoted into the cache tier in the same pass, so no second warm
        // pass re-reads them under the mutex-serialized block store. Callers that only
        // need the index (e.g. the manifest anchor probe) pass warm_cache=false.
        let warm = if warm_cache {
            Some((&self.cache, shard_id))
        } else {
            None
        };
        reconcile_secondary_views_from_bucket_index(&self.page_store, &mut shard, warm);
        // Reloaded data is durable, hence clean: clear the transient per-page/bucket
        // dirty flags that were persisted before unload (the durable dirty_generation
        // identity is preserved). refresh_bucket_runtime_flags below then recomputes
        // dirty purely from the live (empty on load) dirty_objects set. This follows
        // the clear-dirty-on-load contract and keeps `bucket.dirty |= ...` from
        // resurrecting a stale persisted dirty flag.
        for bucket in shard.bucket_index.bucket_map.values_mut() {
            bucket.dirty = false;
            for page in bucket.page_index.mutate().values_mut() {
                page.dirty = false;
            }
        }
        refresh_bucket_runtime_flags(&mut shard);
        Ok(Some(shard))
    }

    /// Fold the append-only index-log deltas onto a (possibly empty) base `ShardState`,
    /// reconstructing the served index for the delta path. Only deltas with a WAL anchor
    /// beyond the base snapshot's own anchor are applied -- deltas already captured by the
    /// base are skipped (their pages may have been relocated/reclaimed by the compaction
    /// that produced the base). Each delta's page items are re-attached at their ORIGINAL
    /// addresses (authoritative replace per covered key) and its per-key non-page state is
    /// applied, then the reconstructed anchor advances so WAL replay re-executes only the
    /// uncaptured (e.g. async) tail rather than relocating the pages the deltas already pin.
    fn fold_index_log_deltas(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
    ) -> Result<(), Status> {
        // Propagate a corrupt/holed delta stream instead of silently folding a prefix. A
        // silently-skipped delta advances the reconstructed anchor past a removal/eviction that
        // lives ONLY in the delta (not the WAL), so it is recovered from neither source -- a
        // silent loss / dangling ref. The caller (load_shard_with) refuses the load on Err.
        let records = match self.index_log_store.read_delta_records(shard_id, 0) {
            Ok(records) => records,
            Err(err) => {
                return Err(Status::error(
                    "index_log_delta_corruption",
                    format!(
                        "served-index delta log for shard {shard_id} is corrupt; refusing load: {err}"
                    ),
                ));
            }
        };
        if records.is_empty() {
            return Ok(());
        }
        let base_anchor = shard.applied_wal_sequence.unwrap_or(0);
        let mut max_anchor = base_anchor;
        let mut applied = false;
        for record in &records {
            let record_anchor = record.applied_wal_sequence.unwrap_or(0);
            // A present base already reflects everything at/below its anchor; fold only the
            // suffix. An absent base (anchor 0) folds the whole log.
            if base_anchor > 0 && record_anchor <= base_anchor {
                continue;
            }
            let covered = delta_record_covered_keys(record);
            fold_delta_page_items(&mut shard.bucket_index, &covered, &record.items, record.upsert);
            apply_key_states(shard, &record.key_states);
            max_anchor = max_anchor.max(record_anchor);
            applied = true;
        }
        if applied {
            for bucket in shard.bucket_index.bucket_map.values_mut() {
                update_bucket_layout(bucket);
            }
            shard.bucket_index.rebuild_object_page_lookup();
            shard.applied_wal_sequence = Some(max_anchor);
        }
        Ok(())
    }

    /// Persist the in-memory shard index to disk once (used by bulk backfill
    /// after driving many extract_context calls under MATRIXARK_BULK_INGEST=1,
    /// which skips per-record persistence). Also refreshes the index-log tail.
    pub fn flush_shard_index(&self, shard_id: ShardId) {
        // Make the chunk's deferred bulk writes durable before publishing the
        // served index: fsync page segments + band manifest, then the WAL. If the
        // barrier FAILS, bail without pinning applied_wal_sequence or writing the index:
        // advancing the durable anchor past pages that never reached disk would suppress
        // their replay on reload -> silent data loss. advances the watermark only after
        // the commit succeeds; the next flush retries.
        if self.page_store.sync_durable().is_err() || self.wal_store.flush(shard_id).is_err() {
            return;
        }
        let index_bytes = {
            // Reconstruct everything the per-command bulk path deferred: promote
            // model-map pages into bucket_index, rebuild the secondary views, refresh
            // runtime flags, then serialize once. Needs a write lock.
            let mut shards = self.shards.write().expect("engine lock poisoned");
            match shards.get_mut(&shard_id) {
                Some(shard) => {
                    if promote_model_maps_to_bucket_index_authority(shard_id, shard, 0, u32::MAX) {
                        reconcile_secondary_views_from_bucket_index(&self.page_store, shard, None);
                    }
                    rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
                    refresh_bucket_runtime_flags(shard);
                    // Anchor the flushed index to the WAL sequence it reflects so a
                    // later load replays only records written after this flush. A
                    // local-context backfill can run beside live hook/proxy writers;
                    // in that mode the batch process may not have applied records
                    // written by the live process, so it pins the anchor to the WAL
                    // sequence observed before the batch began and lets restart
                    // replay the interleaved tail.
                    let default_anchor = self.wal_store.stats(shard_id).last_sequence;
                    let anchor = std::env::var("MATRIXARK_BULK_INGEST_REPLAY_FROM_SEQUENCE")
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .map(|start_sequence| {
                            let expected_tail =
                                std::env::var("MATRIXARK_BULK_INGEST_EXPECTED_WAL_COMMANDS")
                                    .ok()
                                    .and_then(|value| value.parse::<u64>().ok())
                                    .and_then(|written| start_sequence.checked_add(written));
                            if expected_tail == Some(default_anchor) {
                                default_anchor
                            } else {
                                start_sequence.min(default_anchor)
                            }
                        })
                        .unwrap_or(default_anchor);
                    shard.applied_wal_sequence = Some(anchor);
                    serialize_index(shard)
                }
                None => return,
            }
        };
        // Write the served shard index directly, bypassing the bulk-mode gate.
        let _ = fs::create_dir_all(&self.index_dir);
        let _ = atomic_write_bytes(&self.index_path(shard_id), &index_bytes);
    }

    /// MANIFEST-CONFORMANCE FOLD threshold check: dump the index catalog when the undumped index-log
    /// gap has crossed `index_dump_wal_gap_bytes` (the index-meta dump gap
    /// cadence). No-op with the `TS_INDEX_CATALOG_FOLD` gate off, so the background cycle is
    /// byte-identical when the fold is not enabled. Returns whether a dump fired.
    pub fn maybe_dump_index_catalog(&self, shard_id: ShardId) -> bool {
        if !crate::index_log::index_catalog_fold_enabled() {
            return false;
        }
        let gap = crate::storage_config::index_dump_wal_gap_bytes();
        let undumped = self.index_log_store.undumped_len_since_dump(shard_id);
        if !crate::index_log::should_dump_index_catalog(undumped, gap) {
            return false;
        }
        self.dump_index_catalog(shard_id)
    }

    /// Test-only variant of `maybe_dump_index_catalog` taking an explicit gap, so a test can drive
    /// both the below-threshold (no dump) and above-threshold (dump) branches deterministically
    /// without mutating the process-wide gap env mid-test.
    #[cfg(test)]
    pub fn maybe_dump_index_catalog_with_gap_for_test(
        &self,
        shard_id: ShardId,
        gap_bytes: u64,
    ) -> bool {
        if !crate::index_log::index_catalog_fold_enabled() {
            return false;
        }
        let undumped = self.index_log_store.undumped_len_since_dump(shard_id);
        if !crate::index_log::should_dump_index_catalog(undumped, gap_bytes) {
            return false;
        }
        self.dump_index_catalog(shard_id)
    }

    /// MANIFEST-CONFORMANCE FOLD dump: materialize the durable base index, fold the band/zone catalog
    /// into an index-log anchor (the durable band catalog, conformance), and
    /// advance the dumped watermark -- the batched, threshold-driven replacement for the per-write
    /// band-manifest persist. No-op with the gate off. Ordering is single-barrier safe: pages +
    /// WAL are fsynced, then the base index is written durably, then the folded anchor is fsync'd,
    /// and only THEN is the dumped watermark advanced. A crash at any earlier point leaves the
    /// watermark unadvanced, so the next cycle re-dumps rather than trusting a partial dump -- the
    /// catalog is never lost, only (harmlessly) re-materialized. Returns whether a dump completed.
    pub fn dump_index_catalog(&self, shard_id: ShardId) -> bool {
        self.dump_index_catalog_anchored(shard_id).is_some()
    }

    /// [`dump_index_catalog`] returning the dump's coordinates on success: the WAL anchor the
    /// durable base index reflects, and the index-log sequence its folded catalog anchor landed
    /// at. Post-dump log reclaim needs both: everything the base reflects (WAL records at or
    /// below the anchor, index-log records whose own anchor is at or below it) is redundant,
    /// while the folded anchor record itself must survive.
    pub(super) fn dump_index_catalog_anchored(&self, shard_id: ShardId) -> Option<(u64, u64)> {
        if !crate::index_log::index_catalog_fold_enabled() {
            return None;
        }
        // A dump for a shard this engine does not serve is a no-op; check BEFORE the durability
        // barrier so a background caller polling an unloaded shard does not pay (or issue) an
        // fsync for nothing.
        if !self
            .shards
            .read()
            .expect("engine lock poisoned")
            .contains_key(&shard_id)
        {
            return None;
        }
        // 1. Make deferred data pages + the band manifest + the WAL durable BEFORE materializing
        //    the dump, so nothing the anchor references is un-fsynced. Bail (without advancing the
        //    watermark) if the barrier fails; the next cycle retries.
        if self.page_store.sync_durable().is_err() || self.wal_store.flush(shard_id).is_err() {
            return None;
        }
        // 2. Serialize + durably write the base served index (collapses the legacy per-write
        //    whole-index rewrite into this threshold dump). Also read the WAL anchor the index
        //    reflects for the folded MetaItem.
        let (index_bytes, anchor) = {
            let shards = self.shards.read().expect("engine lock poisoned");
            match shards.get(&shard_id) {
                Some(shard) => (
                    serialize_index(shard),
                    shard.applied_wal_sequence.unwrap_or(0),
                ),
                None => return None,
            }
        };
        if self
            .persist_index_bytes_durable(shard_id, &index_bytes)
            .is_err()
        {
            return None;
        }
        // 3. Fold the band/zone catalog into a MetaItem anchor and append it durably to the
        //    index-log. This is this design's "dump the zone catalog into the index log" step:
        //    after it, the band catalog is recoverable from the durable log, so the per-write
        //    band-manifest file stops being the source of truth.
        let zone_version = anchor;
        let zones = self.page_store.zone_catalog(zone_version);
        let meta = crate::index_log::MetaItem {
            version: 1,
            start_wal_sequence: anchor,
            timestamp_ms: now_ms(),
            zone_version,
            zones,
        };
        let meta_sequence = match self.index_log_store.append_delta(
            shard_id,
            Vec::new(),
            Vec::new(),
            Some(anchor),
            Some(meta),
            false,
            true,
        ) {
            Ok(sequence) => sequence,
            Err(_) => return None,
        };
        // 4. The base + folded anchor are durable; advance the dumped watermark so the undumped
        //    gap resets and the next threshold is measured from here.
        self.index_log_store.mark_catalog_dumped(shard_id);
        Some((anchor, meta_sequence))
    }

    /// Embedded-path log maintenance: when the undumped index-log gap has crossed
    /// `index_dump_wal_gap_bytes`, dump the catalog and then RECLAIM what the dump made
    /// redundant. This is what keeps an engine with no background storage-manager cycle (the
    /// gateway proxy embeds the engine directly; only the server/data-node run cycles) from
    /// growing its WAL and index-log without bound: the dump durably captures everything at or
    /// below its anchor, so the logs below it can go. No-op (returning `None`) with the fold
    /// gate off, below the threshold, or when the dump could not complete.
    pub fn maybe_dump_and_reclaim_index_logs(
        &self,
        shard_id: ShardId,
    ) -> Option<super::reports::CatalogDumpReclaimReport> {
        if !crate::index_log::index_catalog_fold_enabled() {
            return None;
        }
        let gap = crate::storage_config::index_dump_wal_gap_bytes();
        let undumped = self.index_log_store.undumped_len_since_dump(shard_id);
        if !crate::index_log::should_dump_index_catalog(undumped, gap) {
            return None;
        }
        self.dump_and_reclaim_index_logs(shard_id)
    }

    /// Unconditional (past the threshold check) dump + reclaim. See
    /// [`maybe_dump_and_reclaim_index_logs`](Self::maybe_dump_and_reclaim_index_logs).
    pub fn dump_and_reclaim_index_logs(
        &self,
        shard_id: ShardId,
    ) -> Option<super::reports::CatalogDumpReclaimReport> {
        let (wal_anchor, meta_sequence) = self.dump_index_catalog_anchored(shard_id)?;
        // Index-log first: drop every record the durable base already reflects (per-record WAL
        // anchor at or below the dump's), keeping the folded catalog anchor and any delta a
        // concurrent writer landed after the dump serialized.
        let index_gc = self
            .index_log_store
            .gc_reflected_before_anchor(shard_id, wal_anchor, meta_sequence)
            .ok();
        // The sweep rewrote (shrank) the log file; re-mark the dumped watermark so the next
        // threshold measures growth from the POST-reclaim length. Without this the gap signal
        // would read `current - pre_reclaim_length`, saturating to zero until the file regrew
        // past its old size -- silently suspending the cadence right after its first success.
        self.index_log_store.mark_catalog_dumped(shard_id);
        // Pages at or below the anchor are in the durable base now, so a log id is no longer
        // the only way to find their bytes. Dropping those entries is what keeps this from
        // growing with the log -- they cost index bytes, and a dumped page will never need one.
        if let Ok(mut shards) = self.shards.write() {
            if let Some(shard) = shards.get_mut(&shard_id) {
                shard
                    .wal_resident_pages
                    .retain(|_, placement| placement.sequence > wal_anchor);
            }
        }
        // Pin the WAL floor to the lowest sequence a live block-in-WAL registration still
        // depends on: such a record holds the ONLY copy of a page the served index points at
        // (the write was acked off a synthetic address), so truncating it turns an acked write
        // into a MISSING read while the process is still serving.
        let wal_retention_floor =
            super::block_in_wal::min_registered_sequence(&self.page_store, shard_id, &self.wal_store);
        match wal_retention_floor {
            Some(sequence) => self.wal_store.set_block_retention_floor(shard_id, sequence),
            None => self.wal_store.clear_block_retention_floor(shard_id),
        }
        // WAL prefix below the anchor: the durable base reflects those records, replay-on-load
        // starts above the anchor, and gc itself additionally retains the highest-sequence
        // record (sequence continuity) and everything at or above the floor just set.
        // The dump above wrote the base served index durably and fsynced the folded catalog
        // anchor on top of it, so the durable index reflects everything at or below
        // `wal_anchor`. That completed work IS the proof this reclaim needs.
        let durable_index =
            crate::wal::DurableIndexAnchor::proven_durable_through(shard_id, wal_anchor);
        let wal_gc = self
            .wal_store
            .gc_before_sequence(shard_id, wal_anchor.saturating_add(1), &durable_index)
            .ok();
        Some(super::reports::CatalogDumpReclaimReport {
            shard_id,
            wal_anchor,
            index_log_records_removed: index_gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            index_log_bytes_before: index_gc
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            index_log_bytes_after: index_gc
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            wal_records_removed: wal_gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            wal_bytes_before: wal_gc
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            wal_bytes_after: wal_gc
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            wal_retention_floor,
        })
    }

    /// Test-only variant of `maybe_dump_and_reclaim_index_logs` taking an explicit gap, so a
    /// test can drive the below-threshold (no-op) and above-threshold (dump + reclaim) branches
    /// deterministically without mutating the process-wide gap env mid-test.
    #[cfg(test)]
    pub fn maybe_dump_and_reclaim_with_gap_for_test(
        &self,
        shard_id: ShardId,
        gap_bytes: u64,
    ) -> Option<super::reports::CatalogDumpReclaimReport> {
        if !crate::index_log::index_catalog_fold_enabled() {
            return None;
        }
        let undumped = self.index_log_store.undumped_len_since_dump(shard_id);
        if !crate::index_log::should_dump_index_catalog(undumped, gap_bytes) {
            return None;
        }
        self.dump_and_reclaim_index_logs(shard_id)
    }

    pub(super) fn persist_index_bytes(
        &self,
        shard_id: ShardId,
        bytes: &[u8],
    ) -> Result<(), std::io::Error> {
        // Bulk backfill defers the served-index rewrite to flush_shard_index()
        // (turns O(n^2) per-record persistence into one write per batch).
        if bulk_ingest_mode() {
            return Ok(());
        }
        fs::create_dir_all(&self.index_dir)?;
        // Ack-path served-index checkpoint. Under the single-barrier default the durable barrier
        // is deferred (content + rename still issued): the WAL is durably synced before ack
        // and replay-on-load rebuilds the served index from it, so a stale-on-crash index
        // only costs a longer WAL replay, never an acked write. This is the served-index
        // fsync removed from the write critical path.
        atomic_write_bytes_synced(&self.index_path(shard_id), bytes, !wal_only_sync())
    }

    /// Unconditional (bulk-gate-bypassing) index write for recovery-critical paths such as
    /// manifest install-on-load. `persist_index_bytes` is deliberately a no-op under bulk
    /// mode to avoid O(n^2) per-record writes, but install is a one-shot recovery step: if
    /// it skips the write, `load_index()` reads the stale pre-manifest index while the
    /// returned watermark advances past the records the manifest embeds -- and replay only
    /// applies records beyond the watermark, so those records (already WAL-GC'd, living only
    /// in the manifest) are permanently lost. Always materialize the restored index here.
    pub(super) fn persist_index_bytes_durable(
        &self,
        shard_id: ShardId,
        bytes: &[u8],
    ) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        atomic_write_bytes(&self.index_path(shard_id), bytes)
    }

    pub(super) fn validate_load_version(
        &self,
        shard_id: ShardId,
        load_version: u64,
    ) -> Result<(), Status> {
        let infos = self.infos.read().expect("info lock poisoned");
        let Some(info) = infos.get(&shard_id) else {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        };
        if !info.loaded {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        }
        if info.load_version != load_version {
            return Err(Status::error(
                "load_version_mismatch",
                format!(
                    "request load_version {} does not match loaded version {}",
                    load_version, info.load_version
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        shards.get(&shard_id).map(|state| {
            let page_store = self.page_store.stats();
            let page_store_zones = self.page_store.zone_summary();
            let string_records = state.strings.len();
            let hash_records = state.hashes.len();
            let set_records = state.sets.len();
            let list_records = state.lists.len();
            let _ = list_records;
            let zset_records = state.zsets.len();
            let _ = zset_records;
            let feature_records = state.features.len();
            let sequence_records = state.sequences.len();
            let control_state_records =
                state.control_state.len() + state.control_state_changes.len();
            let loaded = info.as_ref().map(|info| info.loaded).unwrap_or(true);
            let readonly = info.as_ref().map(|info| info.readonly).unwrap_or(false);
            let load_version = info
                .as_ref()
                .map(|info| info.load_version)
                .unwrap_or_default();
            let table_name = info
                .as_ref()
                .map(|info| info.table_name.clone())
                .unwrap_or_default();
            let shard_uri = info
                .as_ref()
                .map(|info| info.shard_uri.clone())
                .unwrap_or_default();
            let start_routing_bucket = info
                .as_ref()
                .map(|info| info.start_routing_bucket)
                .unwrap_or_default();
            let end_routing_bucket = info
                .as_ref()
                .map(|info| info.end_routing_bucket)
                .unwrap_or(u32::MAX);
            let object_manager =
                object_manager_stats(state, start_routing_bucket, end_routing_bucket);
            let secondary_view_total_records = string_records
                + hash_records
                + set_records
                + feature_records
                + sequence_records
                + control_state_records;
            let total_records = if state.bucket_index.bucket_map.is_empty() {
                secondary_view_total_records
            } else {
                object_manager.object_count
            };
            let shard_stat_info = ShardStatInfo {
                shard_id,
                loaded,
                readonly,
                load_version,
                table_name,
                shard_uri,
                start_routing_bucket,
                end_routing_bucket,
                total_records,
                storage_bytes: page_store.bytes_written,
                object_manager: object_manager.clone(),
            };
            let storage = crate::control::ShardCanonicalStorageStats {
                page_index_entries: object_manager.page_ref_count as u64,
                block_index_entries: page_store.writes,
                object_index_entries: object_manager.object_count as u64,
                bucket_entries: object_manager.routing_bucket_count as u64,
                storage_zone_count: page_store_zones
                    .active_bands
                    .saturating_add(page_store_zones.sealed_bands)
                    .saturating_add(page_store_zones.delayed_destroy_bands)
                    .saturating_add(page_store_zones.purged_bands),
                active_storage_zones: page_store_zones.active_bands,
                sealed_storage_zones: page_store_zones.sealed_bands,
                stream_slab_count: page_store_zones
                    .active_bands
                    .saturating_add(page_store_zones.sealed_bands)
                    .saturating_add(page_store_zones.delayed_destroy_bands)
                    .saturating_add(page_store_zones.purged_bands),
                storage_zone_total_bytes: page_store_zones.total_known_physical_bytes,
                storage_zone_used_bytes: page_store_zones.live_physical_bytes,
                storage_zone_stale_bytes: page_store_zones.reclaimable_physical_bytes,
                page_reads: page_store.reads,
                page_writes: page_store.writes,
                block_reads: page_store.reads,
                block_writes: page_store.writes,
                bytes_read: page_store.bytes_read,
                bytes_written: page_store.bytes_written,
                append_watermark: page_store.writes,
                compaction_watermark: page_store_zones.reclaimable_physical_bytes,
            };
            ShardStats {
                shard_id,
                loaded,
                readonly,
                load_version,
                total_records,
                string_records,
                hash_records,
                set_records,
                feature_records,
                sequence_records,
                control_state_records,
                storage_bytes: page_store.bytes_written,
                object_manager,
                shard_stat_info,
                storage,
                cache: self.cache.stats(),
                page_store: page_store.clone(),
                page_store_zones: page_store_zones.clone(),
                block_store: page_store,
                block_store_bands: page_store_zones,
                write_ahead_log: self.wal_store.stats(shard_id),
            }
        })
    }
}
