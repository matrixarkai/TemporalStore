// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Prometheus metrics rendering for TemporalEngine, split from engine.rs.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

/// TS_METRICS_MAX_BUCKET_SERIES: how many routing buckets may emit per-bucket series on one
/// shard. `TS_METRICS_MAX_SLOT_SERIES` is the previous spelling and is still read.
///
/// A routing bucket is derived per key, so per-bucket metrics scale with record count rather
/// than with topology. Past this many buckets the per-bucket detail is dropped in favour of the
/// shard totals, which are emitted unconditionally.
fn max_bucket_series_per_shard() -> usize {
    crate::env_flag::env_number_first(
        &["TS_METRICS_MAX_BUCKET_SERIES", "TS_METRICS_MAX_SLOT_SERIES"],
        1024,
    )
}

/// How long a scrape may reuse the per-bucket summaries an earlier scrape listed. Per-bucket
/// gauges move on the timescale of a dump, not of a scrape, and every scrape that lists them
/// holds the engine read guard for a walk of the whole shard.
const SCRAPE_BUCKET_SUMMARY_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(test)]
mod scrape_cache_lifecycle_tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const WAIT: Duration = Duration::from_secs(5);
    const FRESH: Duration = Duration::from_secs(3600);

    fn fixture() -> (tempfile::TempDir, TemporalEngine) {
        let root = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            root.path().join("cache"),
            root.path().join("blocks"),
            root.path().join("indexes"),
        );
        (root, engine)
    }

    fn put(engine: &TemporalEngine, shard_id: ShardId, key: &str) {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringSet {
                        key: key.into(),
                        value: b"exact".to_vec()
                    },
                })
                .status
                .ok
        );
    }

    fn blocks(rows: &[BucketStorageSummary]) -> u64 {
        rows.iter().map(|row| row.block_ref_count).sum()
    }

    struct PausedListing {
        entered: mpsc::Receiver<()>,
        release: Option<mpsc::Sender<()>>,
    }

    impl PausedListing {
        fn resume(&mut self) {
            if let Some(sender) = self.release.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for PausedListing {
        fn drop(&mut self) {
            self.resume();
        }
    }

    fn pause_listing(
        engine: TemporalEngine,
        shard_id: ShardId,
    ) -> (
        PausedListing,
        thread::JoinHandle<Arc<Vec<BucketStorageSummary>>>,
    ) {
        let (entered, waiting) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let parked = PausedListing {
            entered: waiting,
            release: Some(release),
        };
        let worker = thread::spawn(move || {
            storage_reporting::on_next_listing_released(move || {
                entered.send(()).unwrap();
                resume.recv().unwrap();
            });
            engine.scrape_bucket_storage_summaries(shard_id, FRESH)
        });
        (parked, worker)
    }

    #[test]
    fn unload_releases_the_cached_listing_without_a_later_scrape() {
        let (_root, engine) = fixture();
        engine.load_shard(1);
        put(&engine, 1, "old");
        let rows = engine.scrape_bucket_storage_summaries(1, FRESH);
        assert_eq!(blocks(&rows), 1);
        let listing = Arc::downgrade(&rows);
        drop(rows);
        assert!(
            listing.upgrade().is_some(),
            "fixture must have populated the cache"
        );
        assert!(
            engine
                .unload_shard_with(UnloadShardRequest { shard_id: 1 })
                .status
                .ok
        );
        assert!(engine.scrape_bucket_summaries.lock().unwrap().is_empty());
        assert!(
            listing.upgrade().is_none(),
            "unload retained the old vector without a later scrape"
        );
    }

    #[test]
    fn owner_replacement_detaches_old_cache_without_removing_the_new_entry() {
        let (_root, engine) = fixture();
        engine.load_shard(1);
        put(&engine, 1, "old");
        let old_owner = engine.shards.handle(1).unwrap();
        let rows = engine.scrape_bucket_storage_summaries(1, FRESH);
        let old_listing = Arc::downgrade(&rows);
        drop(rows);
        let state = engine.shards.with_state(1, |state| state.unwrap().clone());
        engine.install_shard_state(1, state);
        assert!(old_listing.upgrade().is_none());
        assert!(engine.scrape_bucket_summaries.lock().unwrap().is_empty());
        assert!(!old_owner.same_incarnation(&engine.shards.handle(1).unwrap()));
        let replacement = engine.scrape_bucket_storage_summaries(1, FRESH);
        assert_eq!(blocks(&replacement), 1);
        assert!(engine.detach_scrape_bucket_summaries(&old_owner).is_none());
        assert!(Arc::ptr_eq(
            &replacement,
            &engine.scrape_bucket_storage_summaries(1, FRESH)
        ));
    }

    fn stale_refresh_control(replace_owner: bool) {
        let (_root, engine) = fixture();
        engine.load_shard(1);
        put(&engine, 1, "old");
        let original_owner = engine.shards.handle(1).unwrap();
        let (mut pause, old_refresh) = pause_listing(engine.clone(), 1);
        pause
            .entered
            .recv_timeout(WAIT)
            .expect("actual listing did not reach the post-owner pause");
        let old_entry = Arc::downgrade(
            engine
                .scrape_bucket_summaries
                .lock()
                .unwrap()
                .get(&1)
                .unwrap(),
        );
        let lifecycle_engine = engine.clone();
        let (done, completed) = mpsc::channel();
        let lifecycle = thread::spawn(move || {
            if replace_owner {
                assert!(
                    lifecycle_engine
                        .unload_shard_with(UnloadShardRequest { shard_id: 1 })
                        .status
                        .ok
                );
                lifecycle_engine.load_shard(1);
            } else {
                assert!(
                    lifecycle_engine
                        .reload_shard_with(LoadShardRequest {
                            shard_id: 1,
                            load_version: 7,
                            local_node_id: None,
                            shard_uri: String::new(),
                            start_routing_bucket: 0,
                            end_routing_bucket: u32::MAX,
                            readonly: false,
                            table_name: String::new(),
                        })
                        .status
                        .ok
                );
            }
            put(&lifecycle_engine, 1, "new");
            done.send(()).unwrap();
        });
        let progressed = completed.recv_timeout(WAIT);
        if progressed.is_err() {
            pause.resume();
        }
        lifecycle.join().unwrap();
        progressed.expect("lifecycle waited for a parked scrape refresh");
        let current_owner = engine.shards.handle(1).unwrap();
        assert_eq!(
            original_owner.same_incarnation(&current_owner),
            !replace_owner
        );
        assert!(old_entry
            .upgrade()
            .unwrap()
            .invalidated
            .load(Ordering::Acquire));
        assert!(engine.scrape_bucket_summaries.lock().unwrap().is_empty());
        let current_engine = engine.clone();
        let (done, completed) = mpsc::channel();
        let fresh = thread::spawn(move || {
            let rows = current_engine.scrape_bucket_storage_summaries(1, FRESH);
            done.send(rows).unwrap();
        });
        let current = completed.recv_timeout(WAIT);
        if current.is_err() {
            pause.resume();
        }
        fresh.join().unwrap();
        let current = current.expect("replacement cache waited for an obsolete refresh");
        assert_eq!(blocks(&current), 2);
        let replacement_entry = engine
            .scrape_bucket_summaries
            .lock()
            .unwrap()
            .get(&1)
            .unwrap()
            .clone();
        pause.resume();
        assert!(
            old_refresh.join().unwrap().is_empty(),
            "retired refresh returned stale summaries"
        );
        assert!(old_entry.upgrade().is_none());
        assert!(Arc::ptr_eq(
            engine
                .scrape_bucket_summaries
                .lock()
                .unwrap()
                .get(&1)
                .unwrap(),
            &replacement_entry
        ));
        assert!(Arc::ptr_eq(
            &current,
            &engine.scrape_bucket_storage_summaries(1, FRESH)
        ));
    }

    #[test]
    fn stale_refresh_cannot_publish_after_unload_and_same_id_replacement() {
        stale_refresh_control(true);
    }

    #[test]
    fn metadata_reload_invalidates_an_in_flight_scrape_on_the_same_owner() {
        stale_refresh_control(false);
    }

    #[test]
    fn a_parked_refresh_does_not_block_another_shards_scrape_or_write() {
        let (_root, engine) = fixture();
        for id in [1, 2] {
            engine.load_shard(id);
            put(&engine, id, "old");
        }
        let (mut pause, first) = pause_listing(engine.clone(), 1);
        pause
            .entered
            .recv_timeout(WAIT)
            .expect("first listing did not pause");
        let peer_engine = engine.clone();
        let (done, completed) = mpsc::channel();
        let peer = thread::spawn(move || {
            put(&peer_engine, 2, "new");
            done.send(peer_engine.scrape_bucket_storage_summaries(2, FRESH))
                .unwrap();
        });
        let result = completed.recv_timeout(WAIT);
        pause.resume();
        peer.join().unwrap();
        assert_eq!(blocks(&first.join().unwrap()), 1);
        assert_eq!(
            blocks(&result.expect("unrelated shard waited for the parked refresh")),
            2
        );
    }
}

pub(super) struct ScrapeBucketEntry {
    owner: shard_guard::ShardHandle,
    invalidated: AtomicBool,
    summaries: std::sync::Mutex<Option<(std::time::Instant, Arc<Vec<BucketStorageSummary>>)>>,
}

impl std::fmt::Debug for ScrapeBucketEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScrapeBucketEntry")
            .field("shard_id", &self.owner.shard_id())
            .field("invalidated", &self.invalidated.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl TemporalEngine {
    /// The shard's per-bucket summaries for a scrape: the previous scrape's listing when it is
    /// younger than `max_age`, a fresh listing otherwise.
    ///
    /// Refreshes share an incarnation-local mutex, never the cache directory lock.
    pub(super) fn scrape_bucket_storage_summaries(
        &self,
        shard_id: ShardId,
        max_age: std::time::Duration,
    ) -> Arc<Vec<BucketStorageSummary>> {
        let Some(owner) = self.shards.handle(shard_id) else {
            return Arc::new(Vec::new());
        };
        let found = self.shards.with_handle_state(&owner, |state| {
            state.map(|_| {
                self.scrape_bucket_summaries
                    .lock()
                    .expect("scrape summary directory poisoned")
                    .get(&shard_id)
                    .filter(|entry| entry.owner.same_incarnation(&owner))
                    .cloned()
            })
        });
        let entry = match found {
            None => return Arc::new(Vec::new()),
            Some(Some(entry)) => entry,
            Some(None) => {
                let candidate = Arc::new(ScrapeBucketEntry {
                    owner: owner.clone(),
                    invalidated: AtomicBool::new(false),
                    summaries: std::sync::Mutex::new(None),
                });
                let captured = self.shards.with_handle_state(&owner, |state| {
                    state?;
                    let mut entries = self
                        .scrape_bucket_summaries
                        .lock()
                        .expect("scrape summary directory poisoned");
                    if let Some(entry) = entries
                        .get(&shard_id)
                        .filter(|entry| entry.owner.same_incarnation(&owner))
                    {
                        return Some((entry.clone(), None));
                    }
                    let previous = entries.insert(shard_id, candidate.clone());
                    Some((candidate.clone(), previous))
                });
                let Some((entry, retired)) = captured else {
                    return Arc::new(Vec::new());
                };
                drop(retired);
                entry
            }
        };
        // No owner or directory guard is held while waiting for the refresh.
        let mut cache = entry
            .summaries
            .lock()
            .expect("scrape summary refresh poisoned");
        let fresh = self.shards.with_handle_state(&owner, |state| {
            state
                .filter(|_| !entry.invalidated.load(Ordering::Acquire))
                .map(|_| {
                    cache
                        .as_ref()
                        .filter(|(taken, _)| taken.elapsed() < max_age)
                        .map(|(_, summaries)| summaries.clone())
                })
        });
        match fresh {
            None => return Arc::new(Vec::new()),
            Some(Some(summaries)) => return summaries,
            Some(None) => {}
        }
        // The existing builder releases its owner before summary/manifest work.
        let summaries = Arc::new(self.bucket_storage_summaries(shard_id));
        let published = self.shards.with_handle_state(&owner, |state| {
            state
                .filter(|_| !entry.invalidated.load(Ordering::Acquire))
                .map(|_| cache.replace((std::time::Instant::now(), summaries.clone())))
        });
        drop(cache);
        match published {
            Some(previous) => {
                drop(previous);
                summaries
            }
            None => Arc::new(Vec::new()),
        }
    }

    pub(super) fn detach_scrape_bucket_summaries(
        &self,
        owner: &shard_guard::ShardHandle,
    ) -> Option<Arc<ScrapeBucketEntry>> {
        let mut entries = self
            .scrape_bucket_summaries
            .lock()
            .expect("scrape summary directory poisoned");
        if entries
            .get(&owner.shard_id())
            .is_some_and(|entry| entry.owner.same_incarnation(owner))
        {
            entries[&owner.shard_id()]
                .invalidated
                .store(true, Ordering::Release);
            entries.remove(&owner.shard_id())
        } else {
            None
        }
    }

    pub fn prometheus_metrics(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP temporalstore_shard_records Number of records by shard and kind.\n");
        out.push_str("# TYPE temporalstore_shard_records gauge\n");
        out.push_str("# HELP temporalstore_cache_operations_total Cache operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_cache_operations_total counter\n");
        out.push_str("# HELP temporalstore_cache_bytes Cache bytes by shard and tier.\n");
        out.push_str("# TYPE temporalstore_cache_bytes gauge\n");
        out.push_str("# HELP temporalstore_block_store_bytes_total Block store byte counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_block_store_bytes_total counter\n");
        out.push_str("# HELP temporalstore_shard_rate_limit_total Commands allowed and refused by a rate limit. Absent for a shard with no limit, which is not the same as a limit that has refused nothing.\n");
        out.push_str("# TYPE temporalstore_shard_rate_limit_total counter\n");
        out.push_str("# HELP temporalstore_shard_index_lag_records Records appended to the log that the durable index has not yet accounted for, by shard. High values mean a longer restart and reclaim that cannot advance.\n");
        out.push_str("# TYPE temporalstore_shard_index_lag_records gauge\n");
        out.push_str("# HELP temporalstore_shard_expiring_keys Keys holding an expiry deadline that the sweep has not yet removed, by shard. Rising means expiry is falling behind; the sweep's own counts look healthy either way.\n");
        out.push_str("# TYPE temporalstore_shard_expiring_keys gauge\n");
        out.push_str(
            "# HELP temporalstore_wal_records_total Write-ahead log append records by shard.\n",
        );
        out.push_str("# TYPE temporalstore_wal_records_total counter\n");
        out.push_str(
            "# HELP temporalstore_wal_bytes_total Write-ahead log appended bytes by shard.\n",
        );
        out.push_str("# TYPE temporalstore_wal_bytes_total counter\n");
        out.push_str(
            "# HELP temporalstore_object_manager_objects Logical hot objects tracked by shard.\n",
        );
        out.push_str("# TYPE temporalstore_object_manager_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_block_refs Block-address references tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_block_refs gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_objects Dirty logical objects tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_buckets Dirty routing buckets tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_buckets gauge\n");
        out.push_str("# HELP temporalstore_storage_bucket_block_refs Live block refs by shard and routing bucket.\n");
        out.push_str("# TYPE temporalstore_storage_bucket_block_refs gauge\n");
        out.push_str("# HELP temporalstore_storage_bucket_bytes Live bytes by shard, routing bucket, and kind.\n");
        out.push_str("# TYPE temporalstore_storage_bucket_bytes gauge\n");
        out.push_str("# HELP temporalstore_storage_bucket_dirty_objects Dirty objects by shard and routing bucket.\n");
        out.push_str("# TYPE temporalstore_storage_bucket_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_storage_bucket_series_omitted Routing buckets on this shard when per-bucket series were suppressed by TS_METRICS_MAX_BUCKET_SERIES.\n");
        out.push_str("# TYPE temporalstore_storage_bucket_series_omitted gauge\n");
        out.push_str("# HELP temporalstore_block_store_operations_total Canonical block-store operation counters by shard.\n");
        out.push_str("# TYPE temporalstore_block_store_operations_total counter\n");
        out.push_str("# HELP temporalstore_block_store_slab_bytes Canonical block-store slab bytes by shard and kind.\n");
        out.push_str("# TYPE temporalstore_block_store_slab_bytes gauge\n");
        out.push_str("# HELP temporalstore_block_store_slab_count Canonical block-store slab counts by shard and lifecycle state.\n");
        out.push_str("# TYPE temporalstore_block_store_slab_count gauge\n");
        out.push_str("# HELP temporalstore_block_store_slab_oldest_unix_ms Oldest block-store slab timestamp by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_block_store_slab_oldest_unix_ms gauge\n");
        out.push_str("# HELP temporalstore_block_store_slab_oldest_age_ms Oldest block-store slab age by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_block_store_slab_oldest_age_ms gauge\n");
        out.push_str(
            "# HELP temporalstore_partition_routing_buckets Routing buckets owned by shard.\n",
        );
        out.push_str("# TYPE temporalstore_partition_routing_buckets gauge\n");
        out.push_str(
            "# HELP temporalstore_ingestion_records_total Ingestion record counters by outcome.\n",
        );
        out.push_str("# TYPE temporalstore_ingestion_records_total counter\n");
        out.push_str("# HELP temporalstore_ingestion_kafka_lag Kafka ingestion lag in offsets.\n");
        out.push_str("# TYPE temporalstore_ingestion_kafka_lag gauge\n");
        out.push_str("# HELP temporalstore_ingestion_kafka_committed_offset Kafka committed offset by topic and partition.\n");
        out.push_str("# TYPE temporalstore_ingestion_kafka_committed_offset gauge\n");
        out.push_str("# HELP temporalstore_ingestion_stream_committed_sequence Streaming ingestion committed sequence by stream.\n");
        out.push_str("# TYPE temporalstore_ingestion_stream_committed_sequence gauge\n");
        out.push_str("# HELP temporalstore_ingestion_flink_checkpoint_state Flink checkpoint state as a one-hot gauge.\n");
        out.push_str("# TYPE temporalstore_ingestion_flink_checkpoint_state gauge\n");
        // Gathered for every shard before the loop below, which holds a read lock on the shard
        // table: asking per shard inside it takes a second read on that lock, and a writer
        // queued between the two deadlocks.
        let index_lags: std::collections::HashMap<ShardId, u64> =
            self.shard_index_lags().into_iter().collect();
        let expiry_backlogs: std::collections::HashMap<ShardId, u64> =
            self.shard_expiry_backlogs().into_iter().collect();
        for stats in self.loaded_shard_stats() {
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "string".into()),
                ],
                stats.string_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "hash".into()),
                ],
                stats.hash_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "set".into()),
                ],
                stats.set_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "feature".into()),
                ],
                stats.feature_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "sequence".into()),
                ],
                stats.sequence_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "control_state".into()),
                ],
                stats.control_state_records as u64,
            );
            // Only for a shard that carries a limit. A shard with none has nothing to count, and
            // emitting zeros would say "this limit refused nothing" about a shard that has no
            // limit at all -- which is the one distinction an operator needs from this.
            if let Some(waiting) = expiry_backlogs.get(&stats.shard_id) {
                push_metric(
                    &mut out,
                    "temporalstore_shard_expiring_keys",
                    &[("shard_id", stats.shard_id.to_string())],
                    *waiting,
                );
            }
            if let Some(lag) = index_lags.get(&stats.shard_id) {
                push_metric(
                    &mut out,
                    "temporalstore_shard_index_lag_records",
                    &[("shard_id", stats.shard_id.to_string())],
                    *lag,
                );
            }
            if let Some(counters) = self.shard_quota_counters(stats.shard_id) {
                for (kind, value) in [
                    ("read_allowed", counters.read_allowed),
                    ("read_refused", counters.read_refused),
                    ("write_allowed", counters.write_allowed),
                    ("write_refused", counters.write_refused),
                ] {
                    push_metric(
                        &mut out,
                        "temporalstore_shard_rate_limit_total",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("kind", kind.into()),
                        ],
                        value,
                    );
                }
            }
            for (kind, value) in [
                ("memory_hits", stats.cache.memory_hits),
                ("disk_hits", stats.cache.disk_hits),
                ("misses", stats.cache.misses),
                ("puts", stats.cache.puts),
                ("invalidations", stats.cache.invalidations),
                ("memory_evictions", stats.cache.memory_evictions),
                ("pmem_hits", stats.cache.pmem_hits),
                ("pmem_fills", stats.cache.pmem_fills),
                ("pmem_evictions", stats.cache.pmem_evictions),
                (
                    "pmem_admission_accepted",
                    stats.cache.pmem_admission_accepted,
                ),
                (
                    "pmem_admission_rejected",
                    stats.cache.pmem_admission_rejected,
                ),
                ("pmem_eviction_capacity", stats.cache.pmem_eviction_capacity),
                (
                    "pmem_eviction_pinned_skips",
                    stats.cache.pmem_eviction_pinned_skips,
                ),
                (
                    "memory_admission_accepted",
                    stats.cache.memory_admission_accepted,
                ),
                (
                    "memory_admission_rejected",
                    stats.cache.memory_admission_rejected,
                ),
                ("memory_fills", stats.cache.memory_fills),
                ("disk_fills", stats.cache.disk_fills),
                ("refill_failures", stats.cache.refill_failures),
                ("eviction_capacity", stats.cache.eviction_capacity),
                ("eviction_oversize", stats.cache.eviction_oversize),
                ("eviction_cold", stats.cache.eviction_cold),
                ("eviction_low_hit", stats.cache.eviction_low_hit),
                ("eviction_stale", stats.cache.eviction_stale),
                ("ssd_eviction_cold", stats.cache.ssd_eviction_cold),
                ("ssd_eviction_low_hit", stats.cache.ssd_eviction_low_hit),
                ("ssd_eviction_stale", stats.cache.ssd_eviction_stale),
                ("pinned_entries", stats.cache.pinned_entries),
                ("pin_operations", stats.cache.pin_operations),
                ("unpin_operations", stats.cache.unpin_operations),
                ("eviction_pinned_skips", stats.cache.eviction_pinned_skips),
                (
                    "eviction_sampled_groups",
                    stats.cache.eviction_sampled_groups,
                ),
                ("memory_slot_evictions", stats.cache.memory_slot_evictions),
                ("ssd_slot_evictions", stats.cache.ssd_slot_evictions),
                ("compressed_puts", stats.cache.compressed_puts),
                ("compressed_hits", stats.cache.compressed_hits),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (tier, value) in [
                ("memory", stats.cache.memory_bytes),
                ("pmem", stats.cache.pmem_bytes),
                ("disk", stats.cache.disk_bytes),
                ("compression_saved", stats.cache.compression_bytes_saved),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("tier", tier.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("writes", stats.block_store_compat.writes),
                ("reads", stats.block_store_compat.reads),
                (
                    "compressed_writes",
                    stats.block_store_compat.compressed_records_written,
                ),
                ("compressed_reads", stats.block_store_compat.compressed_records_read),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_block_store_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("written", stats.block_store_compat.bytes_written),
                ("read", stats.block_store_compat.bytes_read),
                ("logical_written", stats.block_store_compat.logical_bytes_written),
                ("logical_read", stats.block_store_compat.logical_bytes_read),
                (
                    "compression_saved",
                    stats.block_store_compat.compression_bytes_saved,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_block_store_bytes_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (state, value) in [
                ("active", stats.block_store_slabs_compat.active_slabs),
                ("sealed", stats.block_store_slabs_compat.sealed_slabs),
                (
                    "delayed_destroy",
                    stats.block_store_slabs_compat.delayed_destroy_slabs,
                ),
                ("purged", stats.block_store_slabs_compat.purged_slabs),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_block_store_slab_count",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("state", state.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("active", stats.block_store_slabs_compat.active_physical_bytes),
                ("sealed", stats.block_store_slabs_compat.sealed_physical_bytes),
                (
                    "delayed_destroy",
                    stats.block_store_slabs_compat.delayed_destroy_physical_bytes,
                ),
                ("purged", stats.block_store_slabs_compat.purged_physical_bytes),
                ("live", stats.block_store_slabs_compat.live_physical_bytes),
                (
                    "reclaimable",
                    stats.block_store_slabs_compat.reclaimable_physical_bytes,
                ),
                (
                    "total_known",
                    stats.block_store_slabs_compat.total_known_physical_bytes,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_block_store_slab_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (scope, value) in [
                ("known", stats.block_store_slabs_compat.oldest_known_slab_unix_ms),
                ("live", stats.block_store_slabs_compat.oldest_live_slab_unix_ms),
                (
                    "reclaimable",
                    stats.block_store_slabs_compat.oldest_reclaimable_slab_unix_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_block_store_slab_oldest_unix_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            for (scope, value) in [
                ("known", stats.block_store_slabs_compat.oldest_known_slab_age_ms),
                ("live", stats.block_store_slabs_compat.oldest_live_slab_age_ms),
                (
                    "reclaimable",
                    stats.block_store_slabs_compat.oldest_reclaimable_slab_age_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_block_store_slab_oldest_age_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            push_metric(
                &mut out,
                "temporalstore_wal_records_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.write_ahead_log.writes,
            );
            push_metric(
                &mut out,
                "temporalstore_wal_bytes_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.write_ahead_log.bytes_written,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_block_refs",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.block_ref_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_buckets",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_bucket_count as u64,
            );
            // One routing slot is derived per key, so these are four series per RECORD: measured
            // 20,140 sample lines and 1.6 MB at 5k records, 240,140 lines and 18.9 MB at 60k, with
            // the scrape already at 460 ms. A few million records would build a multi-gigabyte
            // response on every poll. Past the cap the per-slot detail is dropped and the slot
            // count is reported instead; the shard totals emitted above already carry the aggregate.
            // A routing slot is derived per key, so these are four series per RECORD, not per
            // topology: measured 20,140 sample lines and 1.6 MB at 5k records, 240,140 lines and
            // 18.9 MB at 60k. A few million records would build a multi-gigabyte response on every
            // scrape. Past the cap the per-slot detail is dropped and the slot count is reported
            // instead; the shard totals emitted above already carry the aggregate. Note
            // object_manager.routing_bucket_count is the shard's routing RANGE (u32::MAX), not the
            // number of occupied slots, so the occupied count has to come from the summaries.
            let bucket_summaries =
                self.scrape_bucket_storage_summaries(stats.shard_id, SCRAPE_BUCKET_SUMMARY_MAX_AGE);
            let max_bucket_series = max_bucket_series_per_shard();
            if bucket_summaries.len() > max_bucket_series {
                push_metric(
                    &mut out,
                    "temporalstore_storage_bucket_series_omitted",
                    &[("shard_id", stats.shard_id.to_string())],
                    bucket_summaries.len() as u64,
                );
            }
            for summary in bucket_summaries.iter().take(max_bucket_series) {
                push_metric(
                    &mut out,
                    "temporalstore_storage_bucket_block_refs",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("bucket", summary.routing_bucket.to_string()),
                    ],
                    summary.block_ref_count,
                );
                for (kind, value) in [
                    ("logical", summary.logical_bytes),
                    ("physical", summary.physical_bytes),
                ] {
                    push_metric(
                        &mut out,
                        "temporalstore_storage_bucket_bytes",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("bucket", summary.routing_bucket.to_string()),
                            ("kind", kind.to_string()),
                        ],
                        value,
                    );
                }
                push_metric(
                    &mut out,
                    "temporalstore_storage_bucket_dirty_objects",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("bucket", summary.routing_bucket.to_string()),
                    ],
                    summary.dirty_object_count,
                );
            }
            push_metric(
                &mut out,
                "temporalstore_partition_routing_buckets",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.routing_bucket_count as u64,
            );
        }
        let ingestion = self.ingestion_state_report();
        for (outcome, value) in [
            ("accepted", ingestion.stats.accepted_total),
            ("failed", ingestion.stats.failed_total),
            ("duplicate", ingestion.stats.duplicate_total),
            ("dead_letter", ingestion.stats.dead_letter_total),
            (
                "stream_backpressure",
                ingestion.stats.stream_backpressure_total,
            ),
            ("stream_duplicate", ingestion.stats.stream_duplicate_total),
            ("kafka_committed", ingestion.stats.kafka_committed_total),
            ("flink_precommit", ingestion.stats.flink_precommit_total),
            ("flink_commit", ingestion.stats.flink_commit_total),
            ("flink_abort", ingestion.stats.flink_abort_total),
        ] {
            push_metric(
                &mut out,
                "temporalstore_ingestion_records_total",
                &[("outcome", outcome.to_string())],
                value,
            );
        }
        push_metric(
            &mut out,
            "temporalstore_ingestion_kafka_lag",
            &[("scope", "max".to_string())],
            ingestion.stats.max_kafka_lag.max(0) as u64,
        );
        for offset in ingestion.kafka_offsets {
            push_metric(
                &mut out,
                "temporalstore_ingestion_kafka_committed_offset",
                &[
                    ("topic", offset.topic),
                    ("partition", offset.partition.to_string()),
                ],
                offset.committed_offset.max(0) as u64,
            );
        }
        for stream in ingestion.stream_commits {
            push_metric(
                &mut out,
                "temporalstore_ingestion_stream_committed_sequence",
                &[("stream_id", stream.stream_id)],
                stream.committed_sequence,
            );
        }
        for checkpoint in ingestion.flink_checkpoints {
            let status = format!("{:?}", checkpoint.status).to_ascii_lowercase();
            push_metric(
                &mut out,
                "temporalstore_ingestion_flink_checkpoint_state",
                &[
                    ("job_id", checkpoint.job_id),
                    ("operator_uid", checkpoint.operator_uid),
                    ("subtask_index", checkpoint.subtask_index.to_string()),
                    ("checkpoint_id", checkpoint.checkpoint_id.to_string()),
                    ("status", status),
                ],
                1,
            );
        }
        out
    }
}
