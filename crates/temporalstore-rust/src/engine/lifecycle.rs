// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

impl Default for TemporalEngine {
    fn default() -> Self {
        Self::with_cache_and_block_store(MultiLayerCache::default(), LocalBlockStore::default())
    }
}

/// The series a timestamped kind lives in.
///
/// Kept as one place so the apply path and anything else that has to go from a recorded kind to
/// the map it belongs in cannot disagree about the answer.
fn timestamped_series_mut<'a>(
    shard: &'a mut super::state::ShardState,
    kind: &str,
) -> Option<&'a mut std::collections::HashMap<String, std::collections::BTreeMap<u64, crate::block_store::BlockAddress>>>
{
    Some(match kind {
        "feature" => &mut shard.features,
        "context_index" => &mut shard.context_indexes,
        "context_audit" => &mut shard.context_audits,
        "context_child" => &mut shard.context_children,
        "context_summary" => &mut shard.context_summaries,
        "context_compression" => &mut shard.context_compressions,
        _ => return None,
    })
}

#[cfg(test)]
pub(crate) static LAST_REPLAY_WATERMARK: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl TemporalEngine {
    pub fn new(cache: MultiLayerCache) -> Self {
        Self::with_cache_and_block_store(cache, LocalBlockStore::default())
    }

    pub fn with_cache_and_block_store(cache: MultiLayerCache, block_store: LocalBlockStore) -> Self {
        let scratch = crate::scratch::owned_scratch_dir("indexes");
        let mut engine =
            Self::with_cache_block_store_and_index_dir(cache, block_store, scratch.path());
        engine.index_scratch = Some(scratch);
        engine
    }

    pub fn with_cache_block_store_and_index_dir(
        cache: MultiLayerCache,
        block_store: LocalBlockStore,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        let index_dir = index_dir.into();
        let wal_store = LocalWriteAheadLogStore::new(index_dir.join("wals"));
        let index_log_store = LocalIndexLogStore::new(index_dir.join("indexlogs"));
        // Install the durable read-by-address fallback for log-backed hot pages: an eviction
        // handler spills an evicted hot page to a real slab (freeing DRAM + preventing the acked
        // write from reading back as missing). Idempotent + a no-op when disabled.
        hot_page_spill::install_spill_handler(&cache, &block_store);
        Self {
            shards: Arc::default(),
            cache,
            page_store: block_store,
            wal_store,
            index_log_store,
            index_dir,
            index_scratch: None,
            configs: Arc::default(),
            infos: Arc::default(),
            admissions: Arc::default(),
            promote_scans: Arc::default(),
            replay_installs: Arc::default(),
            maintenance_mirror: Arc::default(),
            quotas: Arc::new(RwLock::new(crate::engine::quota::QuotaTable::default())),
        }
    }

    /// How many recorded outcomes recovery has installed (see `replay_installs`).
    pub(crate) fn replay_installs_for_test(&self) -> u64 {
        self.replay_installs
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total per-execute promote reconcile scans this engine has run (see `promote_scans`).
    pub fn promote_scan_count(&self) -> u64 {
        self.promote_scans.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn cache(&self) -> MultiLayerCache {
        self.cache.clone()
    }

    pub fn block_store(&self) -> LocalBlockStore {
        self.page_store.clone()
    }

    #[deprecated(
        since = "0.1.0",
        note = "use block_store; page naming remains only for legacy compatibility"
    )]
    pub fn page_store(&self) -> LocalBlockStore {
        self.block_store()
    }

    pub fn write_ahead_log_store(&self) -> LocalWriteAheadLogStore {
        self.wal_store.clone()
    }

    #[deprecated(
        since = "0.1.0",
        note = "use write_ahead_log_store; wal naming remains only for legacy compatibility"
    )]
    pub fn wal_store(&self) -> LocalWriteAheadLogStore {
        self.write_ahead_log_store()
    }

    pub fn index_log_store(&self) -> LocalIndexLogStore {
        self.index_log_store.clone()
    }

    pub(crate) fn ingestion_dir(&self) -> PathBuf {
        self.index_dir.join("ingestion")
    }

    pub fn with_local_dirs(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        block_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::with_local_dirs_and_block_store_options(
            memory_capacity_bytes,
            cache_dir,
            block_store_dir,
            index_dir,
            BlockStoreOptions::default(),
        )
    }

    pub fn with_local_dirs_and_block_store_options(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        block_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
        block_store_options: BlockStoreOptions,
    ) -> Self {
        Self::with_cache_block_store_and_index_dir(
            MultiLayerCache::new(memory_capacity_bytes, cache_dir),
            LocalBlockStore::with_options(block_store_dir, block_store_options),
            index_dir,
        )
    }

    pub fn load_shard(&self, shard_id: ShardId) {
        let request = LoadShardRequest {
            shard_id,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
            readonly: false,
            table_name: String::new(),
        };
        let _ = self.load_shard_with(request);
    }

    pub fn load_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
        {
            return LoadShardResponse {
                status: Status::error("already_exists", "shard already exists"),
            };
        }
        let (loaded, replay_watermark) = if wal_single_barrier() {
            // SINGLE-BARRIER RECOVERY TRUST (base-only). The data-page + delta fdatasyncs are
            // deferred, so neither the served-index delta nor the anchor it advances can be
            // trusted -- they may reference pages that were never fsync'd. Recover ONLY from
            // durable checkpoints:
            //  * the base index snapshot, materialized durably (fsync) at the last dump/unload --
            //    flush_shard_index fsyncs every page BEFORE advancing its watermark, so every page
            //    at/below the base watermark is on disk; and
            //  * the latest durable dump manifest, if newer than the base file.
            // Then replay the WAL tail from that durable watermark, re-deriving every page written
            // after it (a lost un-synced page is rebuilt, never left dangling) and, via the
            // config-log, re-applying config-driven eviction at the exact frontier. The delta is
            // deliberately NOT folded (load_index_base_only), so each tail record is applied
            // EXACTLY ONCE -- no double-apply of non-idempotent commands (counters, appends).
            let base_only =
                self.load_index_base_only(request.shard_id, eager_cache_warm_on_load());
            let base_watermark = base_only
                .as_ref()
                .and_then(|state| state.applied_wal_sequence)
                .unwrap_or(0);
            match latest_bucket_dump_manifest_at(&self.index_dir, request.shard_id) {
                Some(manifest) if manifest.wal_sequence > base_watermark => {
                    // A durable dump is newer than the base file (base not materialized at that
                    // dump). Use the manifest's embedded durable index as the recovery base. Read
                    // it directly (not install_bucket_dump_manifest) so the stale-manifest guard --
                    // which refuses to install a manifest older than the delta-advanced index-log
                    // sequence -- cannot block trusting the durable checkpoint over the un-synced
                    // delta.
                    match crate::engine::decode_index_bytes(&manifest.index_bytes) {
                        Ok(mut restored) => {
                            rebuild_bucket_page_ownership(
                                request.shard_id,
                                &mut restored,
                                0,
                                u32::MAX,
                            );
                            (Some(restored), manifest.wal_sequence)
                        }
                        Err(_) => (base_only, base_watermark),
                    }
                }
                _ => (base_only, base_watermark),
            }
        } else {
            // If the latest durable dump manifest is newer than the served index, install
            // it as the load base first (recovers data already dumped into a manifest and
            // then WAL-GC'd): the dumped index is restored first, then
            // startup load replays the WAL on top.
            let installed_manifest_watermark =
                match self.install_latest_manifest_if_newer_on_load(request.shard_id) {
                    Ok(watermark) => watermark,
                    // An index-load failure is fatal. A newer durable manifest
                    // that will not install means the served snapshot + (possibly reclaimed)
                    // WAL cannot be trusted to hold the records it covers -- refuse the load.
                    Err(status) => return LoadShardResponse { status },
                };
            // Fold-aware load: a corrupt served-index delta log is fatal here. Silently
            // folding a holed delta prefix would advance the anchor past a removal/eviction
            // recorded only in the delta -> silent loss. Refuse the load instead.
            let loaded = match self.load_index_checked(request.shard_id, eager_cache_warm_on_load()) {
                Ok(loaded) => loaded,
                Err(status) => return LoadShardResponse { status },
            };
            // WAL replay watermark, from the dumped-log id read on startup load:
            // installed manifest -> its wal_sequence; no index
            // file -> 0 (fresh/async-only shard, replay whole retained WAL); anchored index
            // -> its anchor; unanchored (pre-field) index -> current last_sequence (replay
            // nothing, safe upgrade).
            let replay_watermark = match installed_manifest_watermark {
                Some(manifest_watermark) => manifest_watermark,
                None => match &loaded {
                    None => 0,
                    Some(state) => state
                        .applied_wal_sequence
                        .unwrap_or_else(|| self.wal_store.stats(request.shard_id).last_sequence),
                },
            };
            (loaded, replay_watermark)
        };
        // What this load decided to SKIP. A recovery that loses a write loses it by choosing a
        // watermark, and that choice is invisible from outside: the shard loads clean, no error
        // is returned, and the value is simply absent afterwards. Recorded so a failing test can
        // say which watermark it was rather than leaving it to be guessed at.
        #[cfg(test)]
        LAST_REPLAY_WATERMARK.store(replay_watermark, std::sync::atomic::Ordering::SeqCst);
        // MANIFEST-CONFORMANCE FOLD recovery (gate on only): seed the band catalog from the folded
        // band-catalog anchor recovered from the index-log. This is applied AFTER the block
        // store already reconciled its catalog from the durable pages on open, so it only
        // RESTORES the catalog fields a pure disk scan cannot infer (exact lifecycle state,
        // creation/update timestamps, logical byte count, page-id range) and installs bands for
        // reclaimed slabs with no live file. It never deletes a reconciled band and never lowers
        // physical bytes below the slab's real size, so it cannot lose durable state -- it is a
        // metadata refinement over the lossless disk-derived catalog, making the per-write
        // band-manifest file unnecessary as the catalog's source of truth. Off, this is skipped
        // entirely (byte-identical).
        if crate::index_log::index_catalog_fold_enabled() {
            if let Ok(Some(meta)) = self.index_log_store.latest_zone_catalog(request.shard_id) {
                let _ = self.page_store.install_zone_catalog(&meta.zones);
            }
        }
        let mut state = loaded.unwrap_or_default();
        promote_model_maps_to_bucket_index_authority(
            request.shard_id,
            &mut state,
            request.start_routing_bucket,
            request.end_routing_bucket,
        );
        // Publish the info row WITH recovering:true BEFORE inserting into `shards`. A
        // concurrent execute() acquires shards.write() first, so if it observes the shard
        // present it is guaranteed (happens-before via the shards lock) to also observe
        // recovering:true and reject the write. Inserting into `shards` first would open a
        // window where the shard is visible but the info row is absent -> the gate defaults
        // to false -> a concurrent write interleaves with replay (the double-apply this gate
        // exists to prevent).
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_bucket: request.start_routing_bucket,
                end_routing_bucket: request.end_routing_bucket,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: 0,
                replica_membership_version: 0,
                membership_valid: true,
                replica_node_ids: Vec::new(),
                leader_node_id: None,
                // Serving is gated off until replay below completes.
                recovering: true,
            },
        );
        self.shards
            .write()
            .expect("engine lock poisoned")
            .insert(request.shard_id, state);
        self.configs
            .write()
            .expect("config lock poisoned")
            .entry(request.shard_id)
            .or_default();
        // Config is not carried in the served-index checkpoint, so restore the last durably-logged
        // config before replay REGARDLESS of barrier mode. This covers the no-replay path (a clean
        // dump with nothing to tail-replay) and post-restart client writes; the replay loop below
        // overrides it with the WAL-sequence-ordered config while re-driving historical records,
        // then restores the latest again. Without this, a reload defaults `Config` and silently
        // resets feature_max_size + the representation-changing extend gate flags.
        if let Some(entry) = self.config_log_entries(request.shard_id).into_iter().last() {
            self.configs
                .write()
                .expect("config lock poisoned")
                .insert(request.shard_id, entry.config);
        }
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .entry(AdmissionScope::Shard(request.shard_id))
            .or_default();
        // Replay any WAL records not yet reflected in the loaded index, rebuilding
        // in-memory state the way startup load replays the wal. Without
        // this an async_storage write (WAL entry recorded, page/index deferred to the
        // dump) is silently lost on restart if the crash beats the dump.
        // Hand the resolver the log ids the index has been carrying. Without this a page whose
        // only durable copy is a WAL record stays unreadable by address after a reload -- the
        // served index points at a synthetic address and the resolver's table starts empty.
        self.rehydrate_wal_resident_pages(request.shard_id);
        if let Err(status) = self.replay_wal_into_shard(request.shard_id, replay_watermark) {
            // ReplayWal returns DataLoss on a WAL hole and aborts Load. Unwind the
            // partially-loaded shard and refuse the load rather than serve truncated
            // state -- a not-loaded shard is recoverable/re-routable; silent truncation
            // is not.
            self.shards
                .write()
                .expect("engine lock poisoned")
                .remove(&request.shard_id);
            self.infos
                .write()
                .expect("info lock poisoned")
                .remove(&request.shard_id);
            self.configs
                .write()
                .expect("config lock poisoned")
                .remove(&request.shard_id);
            self.admissions
                .write()
                .expect("admission lock poisoned")
                .remove(&AdmissionScope::Shard(request.shard_id));
            return LoadShardResponse { status };
        }
        // Replay succeeded and the shard is consistent: open it for serving. While
        // `recovering` was set, client commands were rejected with a retryable status so no
        // write could interleave with replay and regress the WAL anchor.
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            info.recovering = false;
        }
        // Disk->memory promotion on a normal restart is folded directly into
        // load_index()/reconcile above (gated by eager_cache_warm_on_load()): the pages
        // reconcile reads to rebuild the secondary views are promoted into the cache
        // tier in the same pass, so we avoid a second warm pass re-reading every page
        // under the mutex-serialized block store. No-op on a fresh/empty shard.
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    /// If the latest durable bucket-dump manifest is newer than the served index,
    /// install it (validate + preflight + restore embedded index) and return its
    /// wal_sequence as the WAL replay watermark. `Ok(None)` when nothing newer exists.
    /// `Err` when a newer manifest IS present but will not install: treats
    /// an index-load failure as fatal, because once the served snapshot
    /// lags the manifest the intervening WAL records may already be reclaimed, so a
    /// silent fall-back to the stale snapshot would drop them. The caller refuses the
    /// load instead.
    pub(super) fn install_latest_manifest_if_newer_on_load(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<u64>, Status> {
        let Some(manifest) = latest_bucket_dump_manifest_at(&self.index_dir, shard_id) else {
            return Ok(None);
        };
        let served_anchor = self
            .load_index(shard_id, false)
            .and_then(|state| state.applied_wal_sequence)
            .unwrap_or(0);
        if manifest.wal_sequence <= served_anchor {
            return Ok(None);
        }
        match self.install_bucket_dump_manifest(&manifest) {
            Ok(()) => Ok(Some(manifest.wal_sequence)),
            Err(status) => Err(status),
        }
    }

    /// Replay WAL records with sequence greater than `watermark`, re-driving each
    /// through execute (which rebuilds the bucket index + model maps) WITHOUT
    /// re-appending to the WAL or re-persisting the index per record, then anchor and
    /// persist the reconstructed index once. Matches the WAL replay path,
    /// including its strict sequence-continuity check.
    /// A deterministic rendering of the state outcomes are supposed to reproduce.
    ///
    /// Deliberately not the serialized index: that carries bookkeeping -- the applied watermark,
    /// the log-resident page map -- which legitimately differs between a shard that ran the
    /// commands and one that installed their results. Comparing the maps themselves says
    /// whether the DATA matches, and a mismatch prints as a readable diff rather than as two
    /// blobs of unequal bytes.
    /// The live page entries the bucket index holds, rendered deterministically.
    ///
    /// The typed maps are not the whole shard. The bucket index is durable state that the read
    /// path consults, so a rebuild that gets the maps right and this wrong is still wrong -- and
    /// comparing only the maps would never say so.
    ///
    /// Volatile bookkeeping is deliberately left out: dirty flags, dirty generations and dump
    /// sequences describe what still needs writing out, not what the shard holds, and they
    /// legitimately differ between a shard built by commands and one rebuilt from records.
    pub(super) fn bucket_index_shape_for_test(&self, shard_id: ShardId) -> String {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return String::from("<no shard>");
        };
        let mut out = String::new();
        for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
            for (page_ref_key, page) in &bucket.page_index {
                out.push_str(&format!(
                    "bucket={routing_bucket} ref={page_ref_key} kind={} key={} component={:?} object_id={} slab={} off={} len={} deleted={} log_backed={}
",
                    page.model_id,
                    page.object_key,
                    page.component,
                    page.object_id,
                    page.address.page_slab_id,
                    page.address.offset,
                    page.address.length,
                    page.deleted,
                    page.log_backed,
                ));
            }
        }
        out
    }

    pub(super) fn index_shape_for_test(&self, shard_id: ShardId) -> String {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return String::from("<no shard>");
        };
        let mut out = String::new();
        let mut strings: Vec<_> = shard.strings.iter().collect();
        strings.sort_by(|left, right| left.0.cmp(right.0));
        for (key, address) in strings {
            out.push_str(&format!(
                "string {key} slab={} off={} len={}\n",
                address.page_slab_id, address.offset, address.length
            ));
        }
        for (key, deadline) in &shard.expires_at_ms {
            out.push_str(&format!("expires {key} at={deadline}\n"));
        }
        let mut seen: Vec<_> = shard.seen.iter().collect();
        seen.sort_by(|left, right| left.0.cmp(right.0));
        for (key, set) in seen {
            let mut members: Vec<_> = set.by_member.iter().collect();
            members.sort();
            for (member, at) in members {
                out.push_str(&format!("seen {key} member={member:?} at={at}\n"));
            }
        }
        // Every timestamped series renders the same way, so a kind that stops being installed
        // shows up as a missing line rather than as a map nobody compares.
        let mut entities: Vec<_> = shard.context_entities.iter().collect();
        entities.sort_by(|left, right| left.0.cmp(right.0));
        for (key, members) in entities {
            for (entity_hash, address) in members {
                out.push_str(&format!(
                    "context_entity {key} id={entity_hash} slab={} off={} len={}
",
                    address.page_slab_id, address.offset, address.length
                ));
            }
        }
        let mut counter_pages: Vec<_> = shard.control_state_pages.iter().collect();
        counter_pages.sort_by(|left, right| left.0.cmp(right.0));
        for (key, address) in counter_pages {
            out.push_str(&format!(
                "control_state_page {key} slab={} off={} len={}
",
                address.page_slab_id, address.offset, address.length
            ));
        }
        let mut counters: Vec<_> = shard.control_state.iter().collect();
        counters.sort_by(|left, right| left.0.cmp(right.0));
        for (key, series) in counters {
            for (bucket_ms, total) in series {
                out.push_str(&format!("control_counter {key} at={bucket_ms} total={total}
"));
            }
        }
        let mut selections: Vec<_> = shard.control_state_selection.iter().collect();
        selections.sort_by(|left, right| left.0.cmp(right.0));
        for (key, selected) in selections {
            out.push_str(&format!(
                "control_selection {key} at={} value={:?}
",
                selected.occur_time_ms, selected.value
            ));
        }
        let mut changes: Vec<_> = shard.control_state_changes.iter().collect();
        changes.sort_by(|left, right| left.0.cmp(right.0));
        for (key, buckets) in changes {
            for (bucket_ms, members) in buckets {
                out.push_str(&format!(
                    "control_change {key} at={bucket_ms} members={}
",
                    members.len()
                ));
            }
        }
        let mut event_keys: Vec<_> = shard.context_events.iter().collect();
        event_keys.sort_by(|left, right| left.0.cmp(right.0));
        for (key, entries) in event_keys {
            for (event_id_hash, address) in entries {
                out.push_str(&format!(
                    "context_event {key} id={event_id_hash} slab={} off={} len={}
",
                    address.page_slab_id, address.offset, address.length
                ));
            }
        }
        // The time index is what every windowed read goes through, so a primary map that is
        // right and a timeline that is empty must not compare equal.
        let mut timeline_keys: Vec<_> = shard.context_event_timeline.iter().collect();
        timeline_keys.sort_by(|left, right| left.0.cmp(right.0));
        for (key, entries) in timeline_keys {
            for (timeline_key, event_id_hash) in entries {
                out.push_str(&format!(
                    "context_timeline {key} at={timeline_key} id={event_id_hash}
"
                ));
            }
        }
        for (kind, series) in [
            ("feature", &shard.features),
            ("context_index", &shard.context_indexes),
            ("context_audit", &shard.context_audits),
            ("context_child", &shard.context_children),
            ("context_summary", &shard.context_summaries),
            ("context_compression", &shard.context_compressions),
        ] {
            let mut keys: Vec<_> = series.iter().collect();
            keys.sort_by(|left, right| left.0.cmp(right.0));
            for (key, entries) in keys {
                for (stored_key, address) in entries {
                    out.push_str(&format!(
                        "{kind} {key} at={stored_key} slab={} off={} len={}
",
                        address.page_slab_id, address.offset, address.length
                    ));
                }
            }
        }
        let mut hashes: Vec<_> = shard.hashes.iter().collect();
        hashes.sort_by(|left, right| left.0.cmp(right.0));
        for (key, fields) in hashes {
            let mut entries: Vec<_> = fields.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (field, address) in entries {
                out.push_str(&format!(
                    "hash {key}.{field} slab={} off={}\n",
                    address.page_slab_id, address.offset
                ));
            }
        }
        let mut sets: Vec<_> = shard.sets.iter().collect();
        sets.sort_by(|left, right| left.0.cmp(right.0));
        for (key, members) in sets {
            for (member, address) in members {
                out.push_str(&format!(
                    "set {key} member={member:?} slab={} off={}\n",
                    address.page_slab_id, address.offset
                ));
            }
        }
        let mut lists: Vec<_> = shard.lists.iter().collect();
        lists.sort_by(|left, right| left.0.cmp(right.0));
        for (key, elements) in lists {
            for (sequence, address) in elements {
                out.push_str(&format!(
                    "list {key}[{sequence}] slab={} off={}\n",
                    address.page_slab_id, address.offset
                ));
            }
        }
        let mut zsets: Vec<_> = shard.zsets.iter().collect();
        zsets.sort_by(|left, right| left.0.cmp(right.0));
        for (key, members) in zsets {
            for (member, (score, address)) in members {
                out.push_str(&format!(
                    "zset {key} member={member:?} score={score} slab={} off={}\n",
                    address.page_slab_id, address.offset
                ));
            }
        }
        let mut buckets: Vec<_> = shard.buckets.iter().collect();
        buckets.sort_by(|left, right| left.0.cmp(right.0));
        for (key, (tokens, refilled)) in buckets {
            out.push_str(&format!("bucket {key} tokens={tokens} at={refilled}\n"));
        }
        out
    }

    /// Install one recorded outcome, without running the command that produced it.
    ///
    /// Returns false when the outcome names something this does not know how to install yet, so
    /// a caller can fall back rather than silently skipping state. Kinds are being brought over
    /// one at a time, each gated on an equivalence test, because an apply that quietly drops a
    /// kind rebuilds a shard that is subtly wrong -- which is worse than one that refuses.
    /// Install everything a shared-log entry recorded, or refuse the lot.
    ///
    /// Partial application is the failure this exists to prevent: a successor that installs four
    /// of five items serves a shard that looks whole and is not. Refusing sends the caller back to
    /// re-executing, which is worse but honest.
    pub fn install_shared_outcomes(
        &self,
        shard_id: ShardId,
        outcomes: &[crate::wal::WalOutcomeItem],
    ) -> bool {
        self.install_shared_outcomes_with_blocks(shard_id, outcomes, &[])
    }

    /// Install results, materialising any blocks the entry carried.
    ///
    /// A result names an address in the ORIGIN's block store. On a successor that address means
    /// nothing unless the block store is shared, so an entry that carries its blocks has them
    /// written here and the LOCAL address installed instead. Without this a successor installs a
    /// perfect index over bytes it does not have: every read of the tail returns nothing, and no
    /// error is raised anywhere.
    pub fn install_shared_outcomes_with_blocks(
        &self,
        shard_id: ShardId,
        outcomes: &[crate::wal::WalOutcomeItem],
        carried: &[crate::wal::StagedPage],
    ) -> bool {
        let mut localised = Vec::with_capacity(outcomes.len());
        for item in outcomes {
            let mut item = item.clone();
            if let (Some(address), Some(page)) = (
                item.resolved_address(),
                carried.iter().find(|page| page.object_id == item.object_id),
            ) {
                if self.page_store.read(&address).is_err() {
                    // The bytes are not here, and the entry brought them. Write them and install
                    // where they landed LOCALLY.
                    if let Ok(local) = super::append_value(
                        &self.cache,
                        &self.page_store,
                        shard_id,
                        &page.bytes,
                        Some(item.object_id),
                        Some(item.routing_bucket),
                        false,
                    ) {
                        item.address = Some(local);
                    }
                }
            }
            localised.push(item);
        }
        if localised
            .iter()
            .any(|item| !self.apply_outcome_item(shard_id, item))
        {
            return false;
        }
        let outcomes = &localised[..];
        self.replay_installs.fetch_add(
            outcomes.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        true
    }

    pub(super) fn apply_outcome_item(
        &self,
        shard_id: ShardId,
        item: &crate::wal::WalOutcomeItem,
    ) -> bool {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return false;
        };
        match item.kind.as_str() {
            // The object is gone, everywhere it appeared.
            "object" if item.deleted => {
                super::delete_record(shard, &item.object_key);
                true
            }
            // A deadline, already resolved by the write that set it -- so installing it needs
            // no clock, where re-running the command would resolve it against this one.
            "object" => {
                // No deadline and no deletion means the deadline was CLEARED -- which a write
                // that refreshes a value without a TTL does, and which has to be installable or
                // a lapsed deadline from an earlier record outlives the write that removed it.
                let Some(expires_at) = item.ttl else {
                    for record_key in super::associated_record_keys(&item.object_key) {
                        shard.expires_at_ms.remove(&record_key);
                    }
                    return true;
                };
                for record_key in super::associated_record_keys(&item.object_key) {
                    if super::record_exists_exact(shard, &record_key) {
                        shard.expires_at_ms.insert(record_key, expires_at);
                    }
                }
                true
            }
            // State no page backs: the member and the moment it was seen.
            "seen" => {
                let (Some(member), Some(seen_at)) = (item.value.clone(), item.ttl) else {
                    return false;
                };
                let seen = shard.seen.entry(item.object_key.clone()).or_default();
                if let Some(previous) = seen.by_member.insert(member.clone(), seen_at) {
                    seen.by_time.remove(&(previous, member.clone()));
                }
                seen.by_time.insert((seen_at, member), ());
                true
            }
            // The bucket the take left behind: tokens, then the refill moment.
            "bucket" => {
                let Some(bytes) = item.value.as_ref() else {
                    return false;
                };
                if bytes.len() != 16 {
                    return false;
                }
                let tokens = f64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
                let refilled_at = u64::from_le_bytes(bytes[8..].try_into().expect("8 bytes"));
                shard
                    .buckets
                    .insert(item.object_key.clone(), (tokens, refilled_at));
                true
            }
            "string" => {
                let Some(address) = item.resolved_address() else {
                    return false;
                };
                super::upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "string",
                    &item.object_key,
                    None,
                    address.clone(),
                    true,
                );
                shard.strings.insert(item.object_key.clone(), address);
                true
            }
            // The component is what the map is keyed by, encoded. Each of these mirrors the
            // encoder that produced it; a wrong decode shows up as an equivalence failure, not
            // as a silently different shard.
            "hash" => {
                let (Some(address), Some(field)) = (item.resolved_address(), item.component.clone())
                else {
                    return false;
                };
                super::upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "hash",
                    &item.object_key,
                    Some(field.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .hashes
                    .entry(item.object_key.clone())
                    .or_default()
                    .insert(field, address);
                true
            }
            // set: the component is the member, hex encoded.
            "set" => {
                let (Some(address), Some(component)) =
                    (item.resolved_address(), item.component.clone())
                else {
                    return false;
                };
                let Ok(member) = hex::decode(&component) else {
                    return false;
                };
                super::upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "set",
                    &item.object_key,
                    Some(component),
                    address.clone(),
                    true,
                );
                shard
                    .sets
                    .entry(item.object_key.clone())
                    .or_default()
                    .insert(member, address);
                true
            }
            // list: sixteen hex digits of the sequence, biased so the text sorts in list order.
            "list" => {
                let (Some(address), Some(component)) =
                    (item.resolved_address(), item.component.clone())
                else {
                    return false;
                };
                let Ok(biased) = u64::from_str_radix(&component, 16) else {
                    return false;
                };
                let sequence = biased.wrapping_add(i64::MIN as u64) as i64;
                super::upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "list",
                    &item.object_key,
                    Some(component),
                    address.clone(),
                    true,
                );
                shard
                    .lists
                    .entry(item.object_key.clone())
                    .or_default()
                    .insert(sequence, address);
                true
            }
            // zset: sixteen hex digits of the biased score, then the member in hex.
            "zset" => {
                let (Some(address), Some(component)) =
                    (item.resolved_address(), item.component.clone())
                else {
                    return false;
                };
                if component.len() < 16 {
                    return false;
                }
                let (score_hex, member_hex) = component.split_at(16);
                let (Ok(biased), Ok(member)) =
                    (u64::from_str_radix(score_hex, 16), hex::decode(member_hex))
                else {
                    return false;
                };
                super::upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "zset",
                    &item.object_key,
                    Some(component),
                    address.clone(),
                    true,
                );
                shard
                    .zsets
                    .entry(item.object_key.clone())
                    .or_default()
                    .insert(member, (biased, address));
                true
            }
            // Every timestamped series is the same shape: stored key -> page, with the key in
            // the component. One arm covers all of them, so a new kind is a line in the map
            // below rather than another branch that can be forgotten here.
            //
            // For a feature the key is the point's timestamp, and a trim or a replace arrives
            // as a removal -- which is the whole reason replay can stop consulting the config
            // that decided the trim.
            "feature"
            | "context_index"
            | "context_audit"
            | "context_child"
            | "context_summary"
            | "context_compression" => {
                // No component on a removal means the whole series went, not one point of it.
                if item.deleted && item.component.is_none() {
                    {
                        let Some(series) = timestamped_series_mut(shard, &item.kind) else {
                            return false;
                        };
                        series.remove(&item.object_key);
                    }
                    super::mark_bucket_index_object_deleted(shard, &item.object_key);
                    return true;
                }
                let Some(component) = item.component.clone() else {
                    return false;
                };
                let Ok(stored_key) = component.parse::<u64>() else {
                    return false;
                };
                if !item.deleted && item.address.is_none() {
                    return false;
                }
                // Mutate the series, then hand the SURVIVING pages to the same index sync the
                // write path uses. Registering the page directly here instead looked equivalent
                // and was not: the write path registers a timestamped page with no component,
                // one entry per address, so imitating it with a component produced a bucket
                // index that disagreed about every page while the typed maps matched exactly.
                let live_addresses = {
                    let Some(series) = timestamped_series_mut(shard, &item.kind) else {
                        return false;
                    };
                    let entries = series.entry(item.object_key.clone()).or_default();
                    match item.resolved_address() {
                        Some(address) if !item.deleted => {
                            entries.insert(stored_key, address);
                        }
                        _ => {
                            entries.remove(&stored_key);
                        }
                    }
                    let live = entries.values().cloned().collect::<Vec<_>>();
                    if entries.is_empty() {
                        series.remove(&item.object_key);
                    }
                    live
                };
                super::sync_bucket_index_object_pages(
                    shard,
                    shard_id,
                    &item.kind,
                    &item.object_key,
                    live_addresses,
                    true,
                );
                true
            }
            // context_event: the page is timestamp-keyed but the index entry is keyed by the
            // event id, so the component carries both -- sixteen hex digits of the timeline key,
            // then sixteen of the id. Two maps have to move together: the primary, and the time
            // index that every windowed read goes through. Installing only the primary leaves a
            // shard whose events exist and whose time queries return nothing.
            "context_event" => {
                let Some(component) = item.component.clone() else {
                    return false;
                };
                if component.len() != 32 {
                    return false;
                }
                let (timeline_hex, id_hex) = component.split_at(16);
                let (Ok(timeline_key), Ok(event_id_hash)) = (
                    u64::from_str_radix(timeline_hex, 16),
                    u64::from_str_radix(id_hex, 16),
                ) else {
                    return false;
                };
                if !item.deleted && item.address.is_none() {
                    return false;
                }
                let live_addresses = {
                    let events = shard.context_events.entry(item.object_key.clone()).or_default();
                    match item.resolved_address() {
                        Some(address) if !item.deleted => {
                            events.insert(event_id_hash, address);
                        }
                        _ => {
                            events.remove(&event_id_hash);
                        }
                    }
                    let live = events.values().cloned().collect::<Vec<_>>();
                    let empty = events.is_empty();
                    let timeline = shard
                        .context_event_timeline
                        .entry(item.object_key.clone())
                        .or_default();
                    if item.deleted {
                        timeline.remove(&timeline_key);
                    } else {
                        timeline.insert(timeline_key, event_id_hash);
                    }
                    if timeline.is_empty() {
                        shard.context_event_timeline.remove(&item.object_key);
                    }
                    if empty {
                        shard.context_events.remove(&item.object_key);
                    }
                    live
                };
                super::sync_bucket_index_object_pages(
                    shard,
                    shard_id,
                    "context_event",
                    &item.object_key,
                    live_addresses,
                    true,
                );
                true
            }
            // A context node's page. Its own kind because the write registers no bucket-index
            // entry for it, unlike every other hash page -- so installing it as a "hash" would
            // add an entry the write never made.
            // An entity, under its node's collection. Like the node above, the write registers
            // no bucket-index entry for it, so neither does this.
            // The whole counter series, serialized to one page. The write registers it in the
            // bucket index through the same upsert the string kind uses, so this does too.
            "control_state" => {
                let Some(address) = item.resolved_address() else {
                    return false;
                };
                super::upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "control_state",
                    &item.object_key,
                    None,
                    address.clone(),
                    true,
                );
                shard
                    .control_state_pages
                    .insert(item.object_key.clone(), address);
                true
            }
            "context_entity" => {
                let (Some(address), Some(component)) =
                    (item.resolved_address(), item.component.clone())
                else {
                    return false;
                };
                let Ok(entity_hash) = component.parse::<u64>() else {
                    return false;
                };
                shard
                    .context_entities
                    .entry(item.object_key.clone())
                    .or_default()
                    .insert(entity_hash, address);
                true
            }
            "context_node" => {
                let (Some(address), Some(field)) = (item.resolved_address(), item.component.clone())
                else {
                    return false;
                };
                shard
                    .hashes
                    .entry(item.object_key.clone())
                    .or_default()
                    .insert(field, address);
                true
            }
            // The counter's RESULTING value at one bucket. Installing a result twice is the same
            // result; replaying an increment twice is not, which is the whole reason the record
            // carries the total rather than the delta.
            "control_counter" => {
                let (Some(bytes), Some(component)) = (item.value.as_ref(), item.component.clone())
                else {
                    return false;
                };
                let (Ok(bucket_ms), Ok(total)) = (
                    component.parse::<u64>(),
                    bytes.as_slice().try_into().map(i64::from_le_bytes),
                ) else {
                    return false;
                };
                shard
                    .control_state
                    .entry(item.object_key.clone())
                    .or_default()
                    .insert(bucket_ms, total);
                true
            }
            // A distinct member. Installed through the write path's own producer so the exact-set
            // and sketch representations are chosen the same way they were originally.
            "control_change" => {
                let (Some(value), Some(component)) = (item.value.clone(), item.component.clone())
                else {
                    return false;
                };
                let Ok(bucket_ms) = component.parse::<u64>() else {
                    return false;
                };
                super::hll::record_change(shard, &item.object_key, bucket_ms, value);
                true
            }
            // The value that won a first/last comparison. The winner is installed directly rather
            // than re-comparing against whatever this shard happens to hold.
            "control_selection" => {
                let (Some(value), Some(component)) = (item.value.clone(), item.component.clone())
                else {
                    return false;
                };
                let selection_type = match component.as_str() {
                    "first" => crate::types::ControlStateSelectionType::First,
                    "last" => crate::types::ControlStateSelectionType::Last,
                    _ => return false,
                };
                shard.control_state_selection.insert(
                    item.object_key.clone(),
                    super::state::ControlStateSelectionValue {
                        occur_time_ms: item.ttl.unwrap_or_default(),
                        value,
                        selection_type,
                    },
                );
                true
            }
            _ => false,
        }
    }

    /// The address the index holds for a string key, so a test can compare it against what a
    /// record claims the write did.
    pub(super) fn string_page_address(
        &self,
        shard_id: ShardId,
        key: &str,
    ) -> Option<crate::block_store::BlockAddress> {
        self.shards
            .read()
            .expect("engine lock poisoned")
            .get(&shard_id)
            .and_then(|shard| shard.strings.get(key).cloned())
    }

    /// Write every in-process-only page into the block store, so the index can travel.
    ///
    /// A synthetic address names a page inside a WAL record or in memory. It resolves here and
    /// nowhere else, so an index carrying one is not portable: a checkpoint uploads the slabs the
    /// block store HAS, and a synthetic slab is not among them.
    ///
    /// Called before a checkpoint exports the index. Returns how many were materialised, so a
    /// caller can tell the difference between "none needed it" and "it did not run".
    /// Move the OLDEST log-resident pages into the block store, keeping the most recent
    /// `keep` where they are. Returns how many moved.
    ///
    /// A page whose only durable copy is inside a WAL record costs twice. It holds a registration,
    /// which is memory, and that registration pins `min_registered_sequence` -- reclaim may not
    /// truncate below the lowest one, so a registry that only grows is a log that can never be
    /// reclaimed no matter what the retention policy says. Measured on an ingest of distinct keys,
    /// registrations track writes ONE FOR ONE: 200 writes, 200 held; 1200 writes, 1200 held.
    ///
    /// So the recent stay resident -- they are the ones a read is most likely to want, and their
    /// bytes are in the record just written -- and everything older is written where anyone can
    /// find it. Oldest first, because the oldest is the one holding the floor down.
    pub fn materialize_oldest_resident_pages(&self, shard_id: ShardId, keep: usize) -> usize {
        let ordered = super::block_in_wal::oldest_registered_objects(&self.page_store, shard_id);
        if ordered.len() <= keep {
            return 0;
        }
        let retiring: std::collections::HashSet<u64> = ordered[..ordered.len() - keep]
            .iter()
            .map(|(_, object_id)| *object_id)
            .collect();
        self.materialize_resident_pages_where(shard_id, |object_id| retiring.contains(&object_id))
    }

    pub fn materialize_synthetic_pages(&self, shard_id: ShardId) -> usize {
        self.materialize_resident_pages_where(shard_id, |_| true)
    }

    /// The one implementation both callers use: everything, or only what `wanted` selects.
    fn materialize_resident_pages_where(
        &self,
        shard_id: ShardId,
        wanted: impl Fn(u64) -> bool,
    ) -> usize {
        let addresses: Vec<(String, crate::block_store::BlockAddress)> = {
            let shards = self.shards.read().expect("engine lock poisoned");
            let Some(shard) = shards.get(&shard_id) else {
                return 0;
            };
            shard
                .strings
                .iter()
                .filter(|(_, address)| {
                    crate::wal_record::is_wal_resident(address.page_slab_id)
                })
                .map(|(key, address)| (key.clone(), address.clone()))
                .collect()
        };
        let mut moved = 0usize;
        for (key, address) in addresses {
            let object_id = super::stable_page_object_id(shard_id, "string", &key, None);
            if !wanted(object_id) {
                continue;
            }
            // Read it the way a reader here would -- through the registry that still works in
            // this process -- and write it where anyone can find it.
            let Some(bytes) = super::read_page_bytes(&self.cache, &self.page_store, shard_id, &address)
            else {
                continue;
            };
            let Ok(durable) = super::append_value(
                &self.cache,
                &self.page_store,
                shard_id,
                &bytes,
                Some(object_id),
                address.routing_bucket(),
                false,
            ) else {
                continue;
            };
            {
                let mut shards = self.shards.write().expect("engine lock poisoned");
                if let Some(shard) = shards.get_mut(&shard_id) {
                    shard.strings.insert(key.clone(), durable.clone());
                    super::upsert_bucket_index_page(
                        shard,
                        shard_id,
                        "string",
                        &key,
                        None,
                        durable,
                        true,
                    );
                    // The index no longer names a synthetic slab for this object, so nothing can
                    // resolve through its record any more.
                    shard.wal_resident_pages.remove(&object_id);
                }
            }
            // Retire the registration too. It is what pins the WAL retention floor, and a floor
            // held by a page that is now in the block store stops reclaim for no reason.
            super::block_in_wal::deregister(&self.page_store, shard_id, object_id);
            moved += 1;
        }
        moved
    }

    /// How many log-resident registrations this shard holds.
    pub(crate) fn registration_count_for_test(&self, shard_id: ShardId) -> usize {
        super::block_in_wal::registration_count(&self.page_store, shard_id)
    }

    /// How many addresses in the served index name a slab that is not a file.
    ///
    /// A synthetic slab id means the bytes are inside a WAL record or in memory, resolvable only
    /// through this process's registry. A checkpoint uploads the slabs the block store HAS, so an
    /// address like this cannot travel: it names a place that does not exist anywhere else.
    pub(crate) fn synthetic_address_count_for_test(&self, shard_id: ShardId) -> usize {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return 0;
        };
        let mut count = 0usize;
        let mut check = |address: &crate::block_store::BlockAddress| {
            if crate::wal_record::is_wal_resident(address.page_slab_id) {
                count += 1;
            }
        };
        for address in shard.strings.values() {
            check(address);
        }
        for fields in shard.hashes.values() {
            for address in fields.values() {
                check(address);
            }
        }
        for series in shard.features.values() {
            for address in series.values() {
                check(address);
            }
        }
        for bucket in shard.bucket_index.bucket_map.values() {
            for page in bucket.page_index.values() {
                check(&page.address);
            }
        }
        count
    }

    /// How many WAL-resident page locations this shard's index is carrying.
    ///
    /// The point of the map is that it is part of the index rather than process state, so a
    /// test needs to be able to look at it to say anything about that.
    pub(super) fn wal_resident_page_count(&self, shard_id: ShardId) -> usize {
        self.shards
            .read()
            .expect("engine lock poisoned")
            .get(&shard_id)
            .map(|shard| shard.wal_resident_pages.len())
            .unwrap_or(0)
    }

    /// Re-register every WAL-resident page the index knows about.
    ///
    /// The append path learns a page's log id by writing the record; a reload learns it by
    /// reading the index. Both end up in the same table, which is why no read path had to
    /// change for this to work.
    pub(super) fn rehydrate_wal_resident_pages(&self, shard_id: ShardId) {
        if !super::block_in_wal::enabled() {
            return;
        }
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return;
        };
        for (object_id, placement) in &shard.wal_resident_pages {
            super::block_in_wal::register_at(
                &self.page_store,
                shard_id,
                *object_id,
                placement.log_id,
                placement.sequence,
                &self.wal_store,
            );
        }
    }

    pub(super) fn replay_wal_into_shard(
        &self,
        shard_id: ShardId,
        watermark: u64,
    ) -> Result<(), Status> {
        // Replay both re-executes commands and installs recorded outcomes, and BOTH stage
        // outcome items -- while replay itself never appends, so nothing ever takes them.
        // What it leaves behind is picked up by the next write on this thread and written
        // into that write's record as changes it made itself; a later replay then installs
        // them, and a single one that cannot be applied aborts the whole shard load, taking
        // unrelated keys down with it. Threads are reused across requests in a server and
        // across tests here, so "the next write" is routinely someone else entirely.
        //
        // A guard rather than a drain at the end: every early return below leaks otherwise,
        // and the interesting exits -- an unreadable scan, a refused outcome -- are early
        // returns by construction.
        struct DrainStagedOnExit;
        impl Drop for DrainStagedOnExit {
            fn drop(&mut self) {
                let _ = super::block_in_wal::take_outcomes();
            }
        }
        let _drain_staged = DrainStagedOnExit;

        // A corrupt / unreadable WAL scan is DATA LOSS, not "nothing to replay": swallowing it
        // to Ok(()) would load the shard from the stale base index only, silently discarding
        // the committed WAL tail and defeating the caller's refuse-load-on-DataLoss guard. An
        // absent WAL file is the only "nothing to replay" case, and `scan` already returns an
        // empty vec for it (never an error), so any Err here is a genuine failure -> abort.
        // Start reading past the pieces that hold nothing after the watermark. Reading from the
        // beginning made a restart cost the size of the LOG rather than the size of what was left
        // to replay: every record was read and integrity-checked, and then nearly all of them were
        // dropped by the sequence test below. The answer is conservative -- it never skips a record
        // after the watermark -- so that test still decides what is replayed.
        let start_at = self
            .wal_store
            .log_id_after_sequence(shard_id, watermark)
            .unwrap_or(0);
        let records = match self.wal_store.scan(shard_id, start_at, u64::MAX, u64::MAX) {
            Ok(records) => records,
            Err(err) => {
                return Err(Status::error(
                    "wal_scan_failed",
                    format!(
                        "WAL scan failed during recovery for shard {shard_id}; refusing load rather than serving a truncated prefix: {err}"
                    ),
                ));
            }
        };
        // Decode each record through the integrity-verifying framing decoder and PROPAGATE a
        // failure (a value-preserving bit-flip surfaces as `Corruption`) instead of dropping
        // the record via `.ok()`, which would silently truncate the replayed tail.
        let mut pending: Vec<WriteAheadLogRecord> = Vec::new();
        // Where each record physically lives. The scan hands this back and replay used to drop
        // it (`for (_, line)`), which is why the registration below could not be done at all.
        let mut log_id_by_sequence: std::collections::HashMap<u64, u64> =
            std::collections::HashMap::new();
        for (log_id, line) in records {
            let record = crate::wal::decode_wal_line(&line).map_err(|err| {
                Status::error(
                    "wal_record_corruption",
                    format!(
                        "WAL record integrity failure during recovery for shard {shard_id}; refusing load: {err}"
                    ),
                )
            })?;
            if record.sequence > watermark {
                log_id_by_sequence.insert(record.sequence, log_id);
                pending.push(record);
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        pending.sort_by_key(|record| record.sequence);
        // Drop a trailing atomic batch that never reached its durability barrier. A batch is
        // written as N contiguously-sequenced records sharing one batch_id, buffered, then made
        // durable by a SINGLE barrier; a crash before that barrier can leave a partial suffix on
        // disk. Because the engine assigns the batch a contiguous sequence block and serializes
        // writes, an incomplete batch is always the WAL tail, so truncating it preserves strict
        // sequence continuity for everything before it -- and guarantees the batch is applied
        // all-or-nothing (never a double-applied durable prefix on retry).
        truncate_trailing_incomplete_batch(&mut pending);
        if pending.is_empty() {
            return Ok(());
        }

        // Replay config-driven eviction (feature_max_size trims) with the config that was
        // effective at each record's WAL frontier. An entry stamped `after_seq` is effective for
        // records with sequence > after_seq, so before executing a record we apply every
        // not-yet-applied config entry with `after_seq < record.sequence`. This re-derives the
        // exact historical trim -- no resurrection (default config would skip the trim) and no
        // over-trim/loss (the latest config applied to an older record). Runs in every barrier
        // mode now that the config-log is persisted unconditionally; empty (and inert) for a
        // shard that never had a config change logged.
        let config_log: Vec<ConfigLogEntry> = self.config_log_entries(shard_id);
        let mut config_cursor = 0usize;

        let _guard = WalReplayGuard::enter();
        let mut expected = watermark.saturating_add(1);
        let mut replayed_through = watermark;
        let mut wal_resident_updates: Vec<(u64, crate::engine::state::WalResidentPage)> =
            Vec::new();
        for record in pending {
            // Strict sequence continuity, matching the WAL replay, which
            // returns DataLoss and aborts Load on a hole in the retained WAL. A gap means
            // a WAL record was lost (partial-GC crash / corruption); refuse the load
            // rather than silently serve a truncated prefix.
            if record.sequence != expected {
                return Err(Status::error(
                    "wal_replay_sequence_gap",
                    format!(
                        "WAL replay hole during recovery: expected sequence {expected}, found {}",
                        record.sequence
                    ),
                ));
            }
            while config_cursor < config_log.len()
                && config_log[config_cursor].after_seq < record.sequence
            {
                self.configs
                    .write()
                    .expect("config lock poisoned")
                    .insert(shard_id, config_log[config_cursor].config.clone());
                config_cursor += 1;
            }
            // A page whose only durable copy is INSIDE this record is addressable only if
            // something says where it lives. The write path registered exactly that when it
            // appended; replay registered nothing, so recovery installed outcomes naming pages
            // the successor could not resolve -- and a read for one of them answered None. Not
            // an error, not an empty shard: a durably acknowledged write reported as absent,
            // which is the quietest way a store can lose data. Registering here, where the log
            // id is still in hand, makes a replayed record as addressable as a written one.
            if super::block_in_wal::enabled() && !record.staged_pages.is_empty() {
                if let Some(&log_id) = log_id_by_sequence.get(&record.sequence) {
                    super::block_in_wal::register_record(
                        &self.page_store,
                        shard_id,
                        &record.staged_pages,
                        log_id,
                        record.sequence,
                        &self.wal_store,
                    );
                    // The same fact written where it survives this process, so a later reload
                    // rehydrates it instead of rediscovering that it cannot.
                    wal_resident_updates.extend(record.staged_pages.iter().map(|page| {
                        (
                            page.object_id,
                            crate::engine::state::WalResidentPage {
                                log_id,
                                sequence: record.sequence,
                            },
                        )
                    }));
                }
            }
            // A record that says what its write DID is installed, not re-executed. Re-executing
            // reproduces state only if everything that influenced the original execution is
            // reproduced with it -- which is why the two lines below this exist at all.
            //
            // The fallback is not a nicety. A record carrying no outcomes is replayed as a
            // command exactly as before, so a kind that records nothing recovers correctly
            // instead of silently recovering as nothing. The command cannot be dropped from the
            // record until no accepted write can produce an empty one.
            if !record.outcomes.is_empty() {
                for item in &record.outcomes {
                    if !self.apply_outcome_item(shard_id, item) {
                        return Err(Status::error(
                            "wal_replay_outcome_refused",
                            format!(
                                "WAL replay could not install a recorded {} outcome at sequence {}; refusing load rather than serving a shard missing it",
                                item.kind, record.sequence
                            ),
                        ));
                    }
                }
                self.replay_installs.fetch_add(
                    record.outcomes.len() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                replayed_through = record.sequence;
                expected = expected.saturating_add(1);
                continue;
            }
            // Resolve TTL deadlines / event times against the LEADER's timestamp
            // captured when this record was written, not the (later) restart clock, so
            // recovery reconstructs the identical absolute deadlines the leader logged
            // (resolve-then-log) instead of extending every recently-SETEX'd key.
            set_replay_clock_ms(record.metadata.as_ref().map(|meta| meta.timestamp_ms));
            // Neither results nor an operation. Refusing is the only honest answer: replaying it
            // as nothing would serve a shard missing a durable write and report success.
            let Some(command) = record.command else {
                return Err(Status::error(
                    "wal_replay_record_empty",
                    format!(
                        "WAL record at sequence {} carries neither results nor an operation; refusing load rather than skipping a durable write",
                        record.sequence
                    ),
                ));
            };
            let response = self.execute(ExecuteRequest { shard_id, command });
            if !response.status.ok {
                return Err(Status::error(
                    "wal_replay_failed",
                    format!("WAL replay command at sequence {} failed", record.sequence),
                ));
            }
            replayed_through = record.sequence;
            expected = expected.saturating_add(1);
        }
        if !wal_resident_updates.is_empty() {
            let mut shards = self.shards.write().expect("engine lock poisoned");
            if let Some(shard) = shards.get_mut(&shard_id) {
                for (object_id, placement) in wal_resident_updates {
                    shard.wal_resident_pages.insert(object_id, placement);
                }
            }
        }
        // Restore the latest config for post-recovery client writes: any config entries stamped
        // at/after the last replayed sequence (future-effective changes) were intentionally not
        // applied above.
        while config_cursor < config_log.len() {
            self.configs
                .write()
                .expect("config lock poisoned")
                .insert(shard_id, config_log[config_cursor].config.clone());
            config_cursor += 1;
        }

        if replayed_through > watermark {
            // Only the uncaptured tail beyond the reconstructed delta anchor reaches here
            // (e.g. async writes, which do not append index-log deltas). The delta path's
            // sync writes were already folded at their original addresses in load_index, so
            // this reconstruct handles just the replayed tail.
            let index_bytes = {
                let mut shards = self.shards.write().expect("engine lock poisoned");
                match shards.get_mut(&shard_id) {
                    Some(shard) => {
                        // The per-command model-map -> bucket-index promotion and
                        // first-index rebuild were deferred for every replayed command
                        // (defer_bucket_index_reconstruct() is true under the WalReplayGuard),
                        // which is what turns an O(n^2) reload into O(n). Fold every
                        // replayed record into the bucket index ONCE here, mirroring
                        // flush_shard_index()'s reconstruct-once pass, so serving reads see
                        // the replayed records and the persisted index reflects them.
                        if promote_model_maps_to_bucket_index_authority(
                            shard_id,
                            shard,
                            0,
                            u32::MAX,
                        ) {
                            reconcile_secondary_views_from_bucket_index(
                                &self.page_store,
                                shard,
                                None,
                            );
                        }
                        rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
                        refresh_bucket_runtime_flags(shard);
                        shard.applied_wal_sequence = Some(replayed_through);
                        Some(serialize_index(shard))
                    }
                    None => None,
                }
            };
            if let Some(index_bytes) = index_bytes {
                let _ = self.persist_index_bytes(shard_id, &index_bytes);
            }
        }
        Ok(())
    }

    pub fn unload_shard(&self, shard_id: ShardId) {
        let _ = self.unload_shard_with(UnloadShardRequest { shard_id });
    }

    pub fn unload_shard_with(&self, request: UnloadShardRequest) -> UnloadShardResponse {
        if !self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
        {
            return UnloadShardResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
            };
        }
        // The per-write base rewrite is deferred, so unload materializes the current
        // in-memory index to disk before the shard leaves memory. This keeps a later cold
        // load (and any consumer that reads shard-{id}.index.json directly) on a current
        // base. The index-log is bounded separately by the storage-manager's consumer-aware
        // index GC, so unload does not truncate it here.
        {
            let index_bytes = self
                .shards
                .read()
                .expect("engine lock poisoned")
                .get(&request.shard_id)
                .map(serialize_index);
            if let Some(index_bytes) = index_bytes {
                let _ = self.persist_index_bytes_durable(request.shard_id, &index_bytes);
            }
        }
        self.shards
            .write()
            .expect("engine lock poisoned")
            .remove(&request.shard_id);
        self.infos
            .write()
            .expect("info lock poisoned")
            .remove(&request.shard_id);
        self.configs
            .write()
            .expect("config lock poisoned")
            .remove(&request.shard_id);
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .remove(&AdmissionScope::Shard(request.shard_id));
        // Drop this shard's hot-page spill redirects: on the next load the WAL replay re-derives
        // the hot pages (and re-spills them as needed), so the live-path redirect map stays
        // bounded across load/unload cycles.
        hot_page_spill::clear_shard(request.shard_id);
        // Drop this shard's WAL-resident registrations too: they name records in a log this
        // engine no longer serves, and a reload re-derives them from the WAL anyway.
        block_in_wal::clear_shard(&self.page_store, request.shard_id);
        UnloadShardResponse {
            status: Status::ok(),
        }
    }

    pub fn reload_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        let existing = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let Some(existing) = existing else {
            return self.load_shard_with(request);
        };
        if request.load_version < existing.load_version {
            return LoadShardResponse {
                status: Status::error(
                    "stale_load_version",
                    format!(
                        "reload version {} is older than loaded version {}",
                        request.load_version, existing.load_version
                    ),
                ),
            };
        }
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_bucket: request.start_routing_bucket,
                end_routing_bucket: request.end_routing_bucket,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: existing.membership_version,
                replica_membership_version: existing.replica_membership_version,
                membership_valid: existing.membership_valid,
                replica_node_ids: existing.replica_node_ids,
                leader_node_id: existing.leader_node_id,
                // Reload updates metadata only; it does not replay the WAL, so it serves
                // immediately.
                recovering: false,
            },
        );
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    /// Test-only: run the publish phase of `load_shard_with` (install newer manifest, load
    /// the served-index base, publish the shard as `recovering: true`) but STOP before WAL
    /// replay, leaving the shard parked in the recovery window. Returns the WAL replay
    /// watermark to hand to `test_finish_recovery`. Reuses the exact real recovery helpers so
    /// the observed gate behaviour matches production; it only splits the single synchronous
    /// `load_shard_with` into two steps a single-threaded test can observe between.
    #[cfg(test)]
    pub(crate) fn test_publish_recovering_shard(&self, shard_id: ShardId) -> u64 {
        let installed_manifest_watermark = self
            .install_latest_manifest_if_newer_on_load(shard_id)
            .expect("manifest install should succeed in test");
        let loaded = self.load_index(shard_id, eager_cache_warm_on_load());
        let replay_watermark = match installed_manifest_watermark {
            Some(manifest_watermark) => manifest_watermark,
            None => match &loaded {
                None => 0,
                Some(state) => state
                    .applied_wal_sequence
                    .unwrap_or_else(|| self.wal_store.stats(shard_id).last_sequence),
            },
        };
        let mut state = loaded.unwrap_or_default();
        promote_model_maps_to_bucket_index_authority(shard_id, &mut state, 0, u32::MAX);
        self.infos.write().expect("info lock poisoned").insert(
            shard_id,
            ShardInfo {
                shard_id,
                loaded: true,
                table_name: String::new(),
                shard_uri: String::new(),
                start_routing_bucket: 0,
                end_routing_bucket: u32::MAX,
                readonly: false,
                load_version: 0,
                local_node_id: None,
                membership_version: 0,
                replica_membership_version: 0,
                membership_valid: true,
                replica_node_ids: Vec::new(),
                leader_node_id: None,
                recovering: true,
            },
        );
        self.shards
            .write()
            .expect("engine lock poisoned")
            .insert(shard_id, state);
        self.configs
            .write()
            .expect("config lock poisoned")
            .entry(shard_id)
            .or_default();
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .entry(AdmissionScope::Shard(shard_id))
            .or_default();
        replay_watermark
    }

    /// Test-only: finish the recovery started by `test_publish_recovering_shard` by running
    /// the real WAL replay and then clearing the `recovering` gate, exactly as
    /// `load_shard_with`'s tail does.
    #[cfg(test)]
    pub(crate) fn test_finish_recovery(&self, shard_id: ShardId, watermark: u64) {
        self.replay_wal_into_shard(shard_id, watermark)
            .expect("wal replay should succeed in test");
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&shard_id)
        {
            info.recovering = false;
        }
    }
}

/// Truncate a trailing atomic batch that is missing its commit marker.
///
/// `pending` is the sorted, contiguous WAL tail about to be replayed. The last record is
/// inspected: if it carries batch framing (`batch_id` + `batch_size`) and the batch is not fully
/// present -- fewer than `batch_size` records for that id, or the terminal `batch_index ==
/// batch_size` commit marker is absent -- every record of that trailing batch is dropped. A
/// complete batch (all records present, commit marker last) is kept; a non-batch tail record is
/// left untouched. Interior batches are always complete (an incomplete batch can only be the
/// crash tail), so this never opens a hole before a surviving record.
fn truncate_trailing_incomplete_batch(pending: &mut Vec<WriteAheadLogRecord>) {
    let Some(last) = pending.last() else {
        return;
    };
    let Some((batch_id, batch_size)) = last
        .metadata
        .as_ref()
        .and_then(|meta| meta.batch_id.zip(meta.batch_size))
    else {
        return;
    };
    // Walk back over the contiguous run of records sharing this batch id.
    let mut first_index = pending.len();
    let mut count = 0u32;
    let mut commit_marker_present = false;
    for (index, record) in pending.iter().enumerate().rev() {
        let record_batch = record.metadata.as_ref().and_then(|meta| meta.batch_id);
        if record_batch != Some(batch_id) {
            break;
        }
        first_index = index;
        count = count.saturating_add(1);
        if record
            .metadata
            .as_ref()
            .and_then(|meta| meta.batch_index)
            == Some(batch_size)
        {
            commit_marker_present = true;
        }
    }
    if count < batch_size || !commit_marker_present {
        pending.truncate(first_index);
    }
}

#[cfg(test)]
mod batch_truncation_tests {
    use super::truncate_trailing_incomplete_batch;
    use crate::types::Command;
    use crate::wal::{WriteAheadLogRecord, WriteAheadLogRecordMetadata};

    fn rec(seq: u64, batch: Option<(u64, u32, u32)>) -> WriteAheadLogRecord {
        let command = Command::StringSet {
            key: format!("k{seq}"),
            value: Vec::new(),
        };
        let mut metadata = WriteAheadLogRecordMetadata::single_command(&command);
        if let Some((id, size, index)) = batch {
            metadata.batch_id = Some(id);
            metadata.batch_size = Some(size);
            metadata.batch_index = Some(index);
        }
        WriteAheadLogRecord {
            shard_id: 1,
            sequence: seq,
            command: Some(command),
            metadata: Some(metadata),
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    #[test]
    fn keeps_complete_trailing_batch() {
        let mut pending = vec![rec(1, None), rec(2, Some((7, 2, 1))), rec(3, Some((7, 2, 2)))];
        truncate_trailing_incomplete_batch(&mut pending);
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn drops_incomplete_trailing_batch_missing_commit_marker() {
        // batch id 7 has size 3 but only indexes 1 and 2 persisted (commit marker 3 lost to a
        // crash before the barrier) -> the whole batch is dropped, all-or-nothing.
        let mut pending = vec![rec(1, None), rec(2, Some((7, 3, 1))), rec(3, Some((7, 3, 2)))];
        truncate_trailing_incomplete_batch(&mut pending);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sequence, 1);
    }

    #[test]
    fn keeps_non_batch_tail() {
        let mut pending = vec![rec(1, None), rec(2, None)];
        truncate_trailing_incomplete_batch(&mut pending);
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn keeps_complete_batch_before_non_batch_tail() {
        let mut pending = vec![rec(1, Some((7, 2, 1))), rec(2, Some((7, 2, 2))), rec(3, None)];
        truncate_trailing_incomplete_batch(&mut pending);
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn drops_single_persisted_record_of_larger_batch() {
        // Only the first record of a 3-record batch survived the crash.
        let mut pending = vec![rec(1, None), rec(2, Some((9, 3, 1)))];
        truncate_trailing_incomplete_batch(&mut pending);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sequence, 1);
    }
}
