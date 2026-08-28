// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Prometheus metrics rendering for TemporalEngine, split from engine.rs.
use super::*;

/// TS_METRICS_MAX_SLOT_SERIES: how many routing slots may emit per-slot series on one shard.
///
/// A routing slot is derived per key, so per-slot metrics scale with record count rather than
/// with topology. Past this many slots the per-slot detail is dropped in favour of the shard
/// totals, which are emitted unconditionally.
fn max_slot_series_per_shard() -> usize {
    std::env::var("TS_METRICS_MAX_SLOT_SERIES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(1024)
}

impl TemporalEngine {
    pub fn prometheus_metrics(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP temporalstore_shard_records Number of records by shard and kind.\n");
        out.push_str("# TYPE temporalstore_shard_records gauge\n");
        out.push_str("# HELP temporalstore_cache_operations_total Cache operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_cache_operations_total counter\n");
        out.push_str("# HELP temporalstore_cache_bytes Cache bytes by shard and tier.\n");
        out.push_str("# TYPE temporalstore_cache_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_operations_total Page store operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_operations_total counter\n");
        out.push_str("# HELP temporalstore_page_store_bytes_total Page store byte counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_bytes_total counter\n");
        out.push_str("# HELP temporalstore_page_store_zone_count Page-store zone counts by shard and lifecycle state.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_count gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_bytes Page-store physical bytes by shard and lifecycle kind.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_unix_ms Oldest page-store zone timestamp by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_unix_ms gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_age_ms Oldest page-store zone age by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_age_ms gauge\n");
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
        out.push_str("# HELP temporalstore_object_manager_page_refs Page-address references tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_page_refs gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_objects Dirty logical objects tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_slots Dirty routing slots tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_slots gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_page_refs Live page refs by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_page_refs gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_bytes Live bytes by shard, routing slot, and kind.\n");
        out.push_str("# TYPE temporalstore_storage_slot_bytes gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_dirty_objects Dirty objects by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_series_omitted Routing slots on this shard when per-slot series were suppressed by TS_METRICS_MAX_SLOT_SERIES.\n");
        out.push_str("# TYPE temporalstore_storage_slot_series_omitted gauge\n");
        out.push_str("# HELP temporalstore_block_store_operations_total Canonical block-store operation counters by shard.\n");
        out.push_str("# TYPE temporalstore_block_store_operations_total counter\n");
        out.push_str("# HELP temporalstore_block_store_band_bytes Canonical block-store band bytes by shard and kind.\n");
        out.push_str("# TYPE temporalstore_block_store_band_bytes gauge\n");
        out.push_str(
            "# HELP temporalstore_partition_routing_slots Routing slots owned by shard.\n",
        );
        out.push_str("# TYPE temporalstore_partition_routing_slots gauge\n");
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
                ("writes", stats.page_store.writes),
                ("reads", stats.page_store.reads),
                (
                    "compressed_writes",
                    stats.page_store.compressed_records_written,
                ),
                ("compressed_reads", stats.page_store.compressed_records_read),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
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
                ("written", stats.page_store.bytes_written),
                ("read", stats.page_store.bytes_read),
                ("logical_written", stats.page_store.logical_bytes_written),
                ("logical_read", stats.page_store.logical_bytes_read),
                (
                    "compression_saved",
                    stats.page_store.compression_bytes_saved,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_bytes_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (state, value) in [
                ("active", stats.page_store_zones.active_bands),
                ("sealed", stats.page_store_zones.sealed_bands),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_bands,
                ),
                ("purged", stats.page_store_zones.purged_bands),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_count",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("state", state.into()),
                    ],
                    value,
                );
                push_metric(
                    &mut out,
                    "temporalstore_block_store_band_count",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("state", state.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("active", stats.page_store_zones.active_physical_bytes),
                ("sealed", stats.page_store_zones.sealed_physical_bytes),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_physical_bytes,
                ),
                ("purged", stats.page_store_zones.purged_physical_bytes),
                ("live", stats.page_store_zones.live_physical_bytes),
                (
                    "reclaimable",
                    stats.page_store_zones.reclaimable_physical_bytes,
                ),
                (
                    "total_known",
                    stats.page_store_zones.total_known_physical_bytes,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
                push_metric(
                    &mut out,
                    "temporalstore_block_store_band_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_band_unix_ms),
                ("live", stats.page_store_zones.oldest_live_band_unix_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_band_unix_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_unix_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                    push_metric(
                        &mut out,
                        "temporalstore_block_store_band_oldest_unix_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_band_age_ms),
                ("live", stats.page_store_zones.oldest_live_band_age_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_band_age_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_age_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                    push_metric(
                        &mut out,
                        "temporalstore_block_store_band_oldest_age_ms",
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
                "temporalstore_object_manager_page_refs",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.page_ref_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_slots",
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
            let slot_summaries = self.bucket_storage_summaries(stats.shard_id);
            let max_slot_series = max_slot_series_per_shard();
            if slot_summaries.len() > max_slot_series {
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_series_omitted",
                    &[("shard_id", stats.shard_id.to_string())],
                    slot_summaries.len() as u64,
                );
            }
            for summary in slot_summaries.iter().take(max_slot_series) {
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_page_refs",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_bucket.to_string()),
                    ],
                    summary.page_ref_count,
                );
                for (kind, value) in [
                    ("logical", summary.logical_bytes),
                    ("physical", summary.physical_bytes),
                ] {
                    push_metric(
                        &mut out,
                        "temporalstore_storage_slot_bytes",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("slot", summary.routing_bucket.to_string()),
                            ("kind", kind.to_string()),
                        ],
                        value,
                    );
                }
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_dirty_objects",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_bucket.to_string()),
                    ],
                    summary.dirty_object_count,
                );
            }
            push_metric(
                &mut out,
                "temporalstore_partition_routing_slots",
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
