// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

pub mod golden;
pub mod reports;

mod admin_report;
mod constants;
mod execute_on_shard;
mod context;
mod lifecycle;
mod object_manager;
mod packed_pages;
mod product_model;
mod set_index_serde;
mod zset_index_serde;
mod seen_index_serde;
mod bucket_dump_manifest_methods;
mod storage_lifecycle_methods;
mod storage_manager_cycle;
mod storage_reports;
mod prometheus_metrics;
mod stream_batch_methods;
mod recovery_sweep_compact;
mod persistence;
mod bucket_dump_io;
mod command_validation;
pub mod resource_blobs;
pub mod quota;
pub(crate) mod eviction_sampler;
// Single source of truth for write-command classification (shared with the data_node layer,
// which previously kept a drifted subset that mis-classified context/control-state writes
// as reads -> lifecycle-write-barrier bypass + missing dump scheduling).
pub(crate) use command_validation::{command_object_keys, is_write_command};
pub(crate) use storage_manager_cycle::cross_shard_reclaim_guard_enabled;
mod storage_bucket_internals;
pub use storage_bucket_internals::{
    bucket_page_index_visits, bucket_visit_sites, live_page_scan_entries,
    reset_bucket_page_index_visits, reset_live_page_scan_entries,
};
mod compaction;
mod storage_reporting;
mod hashing;
mod bucket_store;
mod control_rollup;
mod hll;
mod hot_page_spill;
mod block_in_wal;
mod state;

// shared-corpus: storage_bucket_first_physical_index storage_object_manager_bucketstore_runtime_authority storage_model_layout_compaction_policies storage_merged_dump_load_lifecycle storage_object_manager_cold_hot_reload storage_page_address_disk_cache_shared_store_fallback
// shared-corpus: storage_stale_page_density_compaction storage_merged_dump_load_restart_interruption storage_gc_eviction_cold_reads storage_manager_real_pressure_signals storage_manager_wal_reclaim_bucket_generation_retention storage_manager_expire_cursor_scan_limits
// shared-corpus: storage_manager_active_eviction_runtime storage_manager_page_gc_dependency_refusal storage_manager_index_gc_thresholds_recovery storage_control_state_context_page_backed_parity

use self::admin_report::*;
use self::constants::*;
// Re-exported so `wal_record::is_wal_resident` can answer for this sentinel too, rather than
// every site comparing against it by hand.
pub(crate) use self::constants::HOT_PAGE_SLAB_ID;
use self::execute_on_shard::execute_on_shard;
use self::context::*;
use self::packed_pages::*;
use self::product_model::*;
use self::reports::*;
use self::command_validation::*;
use self::compaction::*;
use self::hashing::*;
use self::storage_reporting::*;
use self::storage_bucket_internals::*;
use self::bucket_dump_io::*;
use self::bucket_store::{read_bucket_index_value, bucket_index_component_page_addresses};
use self::state::*;
use crate::block_store::BlockAppendRecord;
use crate::control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, Config, GetConfigResponse, GetInfoResponse, GetStatsResponse,
    LoadShardRequest, LoadShardResponse, MembershipUpdateRequest, ObjectManagerStats,
    ShardStatInfo, ScanStreamRequest, ScanStreamResponse, SetConfigRequest, ShardInfo,
    ShardStats, StreamKind, StreamReadRequest, StreamReadResponse, StreamRecord,
    UnloadShardRequest, UnloadShardResponse,
};
use crate::index_log::LocalIndexLogStore;
use crate::block_store::{LocalBlockStore, BlockAddress, BlockStoreError, BlockStoreGcPolicy, BlockStoreOptions};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ContextCompressionEvent,
    ContextEntity, ContextEvent, ContextIndexRef, ContextNode, ContextPackAudit,
    ContextSummaryVector,
    ContextDirtyNode, EventReplicationMode, EventReplicationSelectionReport,
    ExecuteRequest, ExecuteResponse, FeaturePoint, FeatureWritePolicy, InternalContextIndex,
    ReplicatedBatchExecuteRequest, ReplicatedBatchExecuteResponse,
    ReplicatedExecuteRequest, ControlStateFamily, ControlStateSelectionType, SequenceFeatureRow, SequenceQuerySpec,
    ShardId, Status, StringSetCondition,
};
use crate::wal::{LocalWriteAheadLogStore, WriteAheadLogRecord};
use context::{context_index_ref_identity, validate_context_index_lookup};
use matrixcache::{CacheEntryInfo, CacheGcReport, CacheKey, MultiLayerCache};

#[derive(Debug, Clone)]
pub struct TemporalEngine {
    shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    cache: MultiLayerCache,
    page_store: LocalBlockStore,
    wal_store: LocalWriteAheadLogStore,
    index_log_store: LocalIndexLogStore,
    index_dir: PathBuf,
    // Set only when the engine minted its own index_dir (no caller-supplied one): the
    // engine owns that scratch directory, and the last clone's drop removes it.
    index_scratch: Option<Arc<crate::scratch::ScratchDirGuard>>,
    configs: Arc<RwLock<HashMap<ShardId, Config>>>,
    infos: Arc<RwLock<HashMap<ShardId, ShardInfo>>>,
    admissions: Arc<RwLock<HashMap<AdmissionScope, AdmissionState>>>,
    /// Per-shard read and write rate limits. Empty and inert unless something sets a limit, or the
    /// environment carries a default.
    quotas: Arc<RwLock<quota::QuotaTable>>,
    /// Diagnostics: number of per-execute `promote_model_maps_to_bucket_index_authority` full
    /// O(store) reconcile scans this engine has run at the hot-path call site. Without
    /// TS_PHASE1_FLAT this fires once per command (O(writes)); with the gate on the
    /// `promote_scan_done` fast-skip holds it to a small constant once warm. Read by the phase-1
    /// aging test to prove the per-write O(n) reconcile scan is gone.
    promote_scans: Arc<std::sync::atomic::AtomicU64>,
    /// Diagnostics: how many recorded outcomes this engine has INSTALLED during WAL replay.
    ///
    /// Recovery falls back to re-executing a record's command when it carries no outcomes, so a
    /// restart test comparing shard shapes passes either way and proves nothing about which path
    /// ran. This makes the claim checkable: a test can require that recovery installed what the
    /// writes recorded rather than quietly replaying commands again.
    replay_installs: Arc<std::sync::atomic::AtomicU64>,
    /// Where to mirror writes this engine performs OUTSIDE the request path.
    ///
    /// Request-path writes are mirrored a layer up, by the data node, which sees each command
    /// as it arrives. Maintenance never passes through there: eviction and the expiry sweep
    /// append their own tombstones straight to the WAL. In shared mode those deletions therefore
    /// reached the local log and no other, so a successor replaying the shared log never saw
    /// them and the key came back -- the same failure the tombstone was introduced to fix, one
    /// level up from where it was fixed.
    maintenance_mirror: Arc<RwLock<Option<Arc<dyn crate::data_node::SharedWalSink>>>>,
}

impl TemporalEngine {
    /// Mirror the deletions this engine emits on its own -- eviction drops, expiry sweeps --
    /// to the same place request-path writes go.
    ///
    /// Opt-in. With nothing attached the engine behaves exactly as it did.
    pub fn set_maintenance_wal_mirror(&self, sink: Arc<dyn crate::data_node::SharedWalSink>) {
        *self
            .maintenance_mirror
            .write()
            .expect("maintenance mirror lock poisoned") = Some(sink);
    }

    /// Hand a maintenance-generated command to the mirror, if one is attached.
    ///
    /// Called after the local append succeeds, so the mirror never learns of a deletion the
    /// local log does not already hold.
    pub(crate) fn mirror_maintenance_write(&self, shard_id: ShardId, command: &Command) {
        let sink = self
            .maintenance_mirror
            .read()
            .expect("maintenance mirror lock poisoned")
            .clone();
        if let Some(sink) = sink {
            sink.record_write(shard_id, command);
        }
    }

    /// Set a shard's read and write rate limits, on a running engine.
    ///
    /// A rate of zero leaves that direction unlimited. Replacing a limit rebuilds the bucket, so
    /// credit accumulated under the previous rate is dropped -- credit earned at one rate does not
    /// mean anything at another.
    pub fn set_shard_quota(&self, shard_id: ShardId, config: quota::ShardQuotaConfig) {
        self.quotas
            .write()
            .expect("quota lock poisoned")
            .set(shard_id, config);
    }

    /// What a shard's limits currently are, if any were set.
    pub fn shard_quota(&self, shard_id: ShardId) -> Option<quota::ShardQuotaConfig> {
        self.quotas
            .read()
            .expect("quota lock poisoned")
            .config_of(shard_id)
    }

    /// How far behind the log each loaded shard's durable index is, in records.
    ///
    /// Records land in the log first and the index accounts for them afterwards; this is the
    /// distance between. It explains two symptoms that otherwise look unrelated to each other: a
    /// restart that takes much longer than usual, because everything past the index anchor is
    /// replayed, and reclaim that frees nothing, because it will not pass that anchor.
    ///
    /// Every shard in one pass. Callers that already hold a read lock on the shard table must not
    /// ask per shard: a second read on the same lock, with a writer queued between them, deadlocks.
    pub fn shard_index_lags(&self) -> Vec<(ShardId, u64)> {
        let applied: Vec<(ShardId, u64)> = {
            let shards = self.shards.read().expect("engine lock poisoned");
            shards
                .iter()
                .map(|(shard_id, shard)| (*shard_id, shard.applied_wal_sequence.unwrap_or(0)))
                .collect()
        };
        applied
            .into_iter()
            .map(|(shard_id, applied)| {
                let appended = self.wal_store.cached_last_sequence(shard_id);
                (shard_id, appended.saturating_sub(applied))
            })
            .collect()
    }

    /// How many keys each loaded shard is holding an expiry deadline for.
    ///
    /// The sweep's own report says what it REMOVED, which looks equally healthy whether the backlog
    /// behind it is draining or growing. This says which. The engine already decides how hard to
    /// work from this number -- it becomes the expiry component of the storage cycle's pressure
    /// signal -- so publishing it only makes visible what is already being acted on.
    ///
    /// Every shard in one pass, for the same reason as the trailing distance: a caller inside the
    /// metrics loop already holds a read lock on the shard table.
    pub fn shard_expiry_backlogs(&self) -> Vec<(ShardId, u64)> {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .iter()
            .map(|(shard_id, shard)| (*shard_id, shard.expires_at_ms.len() as u64))
            .collect()
    }

    /// What a shard's rate limit has allowed and refused, if it has one.
    ///
    /// Absent means the shard is not limited, which is different from a limit that has refused
    /// nothing -- and the difference is the one an operator actually wants.
    pub fn shard_quota_counters(&self, shard_id: ShardId) -> Option<quota::QuotaCounters> {
        self.quotas
            .read()
            .expect("quota lock poisoned")
            .counters_of(shard_id)
    }

    /// Every shard carrying a rate limit, for reporting.
    pub fn rate_limited_shards(&self) -> Vec<ShardId> {
        self.quotas
            .read()
            .expect("quota lock poisoned")
            .limited_shards()
    }

    /// Take one token for `kind`. True when the command may proceed.
    ///
    /// The overwhelmingly common case is a shard with no limit, and that case must not pay for
    /// this. The environment default is read once for the process rather than per command, and a
    /// shard that is not limited settles under a READ lock -- taking the write lock on every
    /// command would serialise the engine on a feature almost nobody has turned on.
    fn charge_quota(&self, shard_id: ShardId, kind: quota::QuotaKind) -> bool {
        static DEFAULT: std::sync::OnceLock<quota::ShardQuotaConfig> = std::sync::OnceLock::new();
        let default = *DEFAULT.get_or_init(quota::ShardQuotaConfig::from_env);
        if default.is_unlimited() {
            let table = self.quotas.read().expect("quota lock poisoned");
            if !table.limits(shard_id) {
                return true;
            }
        }
        self.quotas
            .write()
            .expect("quota lock poisoned")
            .try_consume(shard_id, kind, default)
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, None, Vec::new())
    }

    /// Apply `request`, attaching `pages` to its log record instead of whatever this node
    /// would derive for it.
    ///
    /// For replaying a write that was already acked somewhere else. A page can be derived
    /// state -- a serialized counter series, a hash map -- and re-executing the command that
    /// produced it reconstructs it only from a state this node may no longer have. When the
    /// original bytes travelled with the command, they are the truth, and re-deriving would
    /// quietly substitute a reconstruction for what was actually acknowledged.
    ///
    /// An empty `pages` is exactly [`execute`](Self::execute).
    pub fn execute_with_carried_pages(
        &self,
        request: ExecuteRequest,
        pages: Vec<crate::wal::StagedPage>,
    ) -> ExecuteResponse {
        self.execute_with_storage_override(request, None, pages)
    }

    pub fn execute_durable(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, Some(false), Vec::new())
    }

    /// Apply a committed raft entry to the state machine, durably (fsync'd WAL) but with a
    /// NON-BLOCKING index-log append: on the raft path the raft log is the durability +
    /// reconstruction source, so the per-apply index-log fsync is redundant. Removing it off
    /// the critical replication path shortens apply latency (which otherwise widens the
    /// snapshot-transfer / backpressure window). A crash that loses the non-fsync'd index-log
    /// tail is safe -- raft-log replay on restart re-applies and rebuilds the served index.
    pub fn execute_raft_apply(&self, request: ExecuteRequest) -> ExecuteResponse {
        let _guard = RaftApplyGuard::enter();
        self.execute_with_storage_override(request, Some(false), Vec::new())
    }

    /// Apply a batch of committed raft entries to the state machine. Under
    /// `TS_RAFT_APPLY_COALESCE` the per-entry engine-WAL fdatasync is coalesced into ONE barrier
    /// for the whole batch (an AppendEntries batch on a follower, a recovery replay, or a
    /// pipelined-propose group): every entry appends its WAL bytes with sync=false and RESERVES its
    /// sequence, then a single `commit_barrier` makes the whole batch durable. The raft log stays
    /// the durability + reconstruction source, and the coalesced barrier still completes here --
    /// inside apply -- BEFORE the raft runtime advances the durable `applied_index`
    /// (persist_configured_wal runs after apply), so `applied => engine-WAL-durable` holds exactly
    /// as with the per-entry path; a crash before the barrier leaves applied_index below the batch
    /// so raft replay re-applies it. Gate OFF (or a single-entry batch) -> a plain per-entry
    /// `execute_raft_apply` loop (byte-identical).
    pub fn execute_raft_apply_batch(&self, requests: Vec<ExecuteRequest>) -> Vec<ExecuteResponse> {
        if !raft_apply_coalesce() || requests.len() <= 1 {
            return requests
                .into_iter()
                .map(|request| self.execute_raft_apply(request))
                .collect();
        }
        let _apply_guard = RaftApplyGuard::enter();
        let batch_guard = RaftApplyBatchGuard::enter();
        let mut responses = Vec::with_capacity(requests.len());
        for request in requests {
            responses.push(self.execute_with_storage_override(request, Some(false), Vec::new()));
        }
        let barrier = batch_guard.take_barrier();
        drop(batch_guard);
        if let Some((shard_id, sequence)) = barrier {
            if let Err(err) = self.wal_store.commit_barrier(shard_id, sequence) {
                // The coalesced batch barrier failed: none of these writes are durable. Fail every
                // otherwise-ok response so raft apply surfaces the durability failure instead of
                // acking (mirrors the single-write commit_barrier failure path).
                for response in responses.iter_mut() {
                    if response.status.ok {
                        *response = ExecuteResponse {
                            status: Status::error(
                                "wal_commit_failed",
                                format!("durable WAL commit barrier failed: {err}"),
                            ),
                            response: CommandResponse::Empty,
                        };
                    }
                }
            }
        }
        responses
    }

    pub fn execute_replicated(&self, request: ReplicatedExecuteRequest) -> ExecuteResponse {
        let replication_mode = request.replication_mode;
        let request = ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        };
        match replication_mode {
            EventReplicationMode::SyncStorage => {
                self.execute_with_storage_override(request, Some(false), Vec::new())
            }
            EventReplicationMode::AsyncStorage => {
                self.execute_with_storage_override(request, Some(true), Vec::new())
            }
            EventReplicationMode::Raft | EventReplicationMode::Inherit => self.execute(request),
        }
    }

    pub fn replication_selection_report(
        &self,
        command: &Command,
        requested_mode: EventReplicationMode,
    ) -> EventReplicationSelectionReport {
        let write_command = is_write_command(command);
        let effective_mode = if write_command {
            requested_mode
        } else {
            EventReplicationMode::Inherit
        };
        EventReplicationSelectionReport {
            requested_mode,
            effective_mode,
            write_command,
            accepted: true,
            restart_required: requested_mode.requires_restart(),
            reason: if !write_command {
                "read_command_does_not_replicate".to_string()
            } else if requested_mode == EventReplicationMode::Inherit {
                "using_current_runtime_default_without_restart".to_string()
            } else {
                "event_selected_replication_mode_without_restart".to_string()
            },
        }
    }

    fn execute_with_storage_override(
        &self,
        request: ExecuteRequest,
        async_storage_override: Option<bool>,
        mut carried_pages: Vec<crate::wal::StagedPage>,
    ) -> ExecuteResponse {
        // Charged before anything else, including the read-only fast path -- a read served without
        // taking the shard lock still costs the shard, and a limit the cheapest reads slip past is
        // not a limit.
        //
        // Not charged while applying a replicated entry or replaying the log. Refusing either is
        // not shedding load: a follower that rejects what the leader committed diverges from it,
        // and a replay that rejects a record already in the log cannot rebuild the shard.
        if !raft_applying() && !replaying_wal() {
            let kind = if command_validation::is_write_command(&request.command) {
                quota::QuotaKind::Write
            } else {
                quota::QuotaKind::Read
            };
            if !self.charge_quota(request.shard_id, kind) {
                return ExecuteResponse {
                    status: Status::error(
                        "quota_exhausted",
                        format!(
                            "shard {} is over its {} rate limit",
                            request.shard_id,
                            match kind {
                                quota::QuotaKind::Write => "write",
                                quota::QuotaKind::Read => "read",
                            }
                        ),
                    ),
                    response: CommandResponse::Empty,
                };
            }
        }
        // Blob commands run before the shard lock: blobs live beside the engine, not inside
        // any shard's record state, and a large upload must never hold the shard write lock.
        if let Some(response) = self.execute_resource_blob_command(&request) {
            return response;
        }
        if async_storage_override.is_some() {
            if let Some(response) = self.execute_read_only_fast_path(&request) {
                return response;
            }
        }
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            };
        };
        // While a shard is replaying its WAL on load it is present in `shards` but not yet
        // serving (keeps it in PartitionLoadStage::LOADING). Reject client commands with
        // a retryable status so a concurrent write cannot interleave with replay -- which
        // would regress the WAL anchor and double-apply on the next restart. The replay
        // thread re-drives records under replaying_wal(), which bypasses this gate.
        if !replaying_wal()
            && self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&request.shard_id)
                .map(|info| info.recovering)
                .unwrap_or(false)
        {
            return ExecuteResponse {
                status: Status::error(
                    "shard_not_loaded",
                    "shard is recovering (WAL replay in progress)",
                ),
                response: CommandResponse::Empty,
            };
        }
        let command = request.command;
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false)
            && is_write_command(&command)
        {
            return ExecuteResponse {
                status: Status::error("readonly_shard", "readonly shard rejects write command"),
                response: CommandResponse::Empty,
            };
        }
        let mut config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        if let Some(async_storage) = async_storage_override {
            config.async_storage = async_storage;
        }
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let start_routing_bucket = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end_routing_bucket = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        // Bulk backfill AND WAL replay defer this model-map -> bucket-index promotion
        // (an O(store) scan plus secondary-view rebuild) to a single reconstruct pass
        // (flush_shard_index() / replay_wal_into_shard()'s tail). Run per command it is
        // the dominant O(n^2) cost of a large ingest/reload; fresh writes live in the
        // model maps, so the single reconstruct rebuilds bucket_index and the secondary
        // views losslessly.
        // Phase-1 flat-append fast-skip: once a promote scan has confirmed `bucket_index` is in
        // sync with the model maps, skip the O(store) re-scan on every subsequent command. The
        // live write path keeps `bucket_index` authoritative in lock-step (each mutating command
        // upserts its page before returning), so the repeat scan can only re-confirm sync. The
        // flag is `#[serde(skip)]` (false on any fresh load), so the first live command after a
        // reload still pays one full reconcile. Gate OFF -> the scan runs every command as before.
        if !defer_bucket_index_reconstruct()
            && !(phase1_flat_enabled() && shard.promote_scan_done)
        {
            self.promote_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if promote_model_maps_to_bucket_index_authority(
                request.shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
            ) {
                reconcile_secondary_views_from_bucket_index(&self.page_store, shard, None);
            }
            // Mark the reconcile confirmed only once the shard actually holds model-map state:
            // `promote` returns false (without establishing anything) on an empty shard, so
            // guarding on non-emptiness avoids latching the flag before the first real write.
            if phase1_flat_enabled() && shard_has_model_entries(shard) {
                shard.promote_scan_done = true;
            }
        }
        let write_command = is_write_command(&command);
        if let Err(status) = self.check_admission(request.shard_id, write_command, &config, &info) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        if write_command
            && config
                .maxmemory_bytes
                // Gate on the CURRENT on-disk footprint (decremented by GC/compaction/
                // purge), not the cumulative-ever `bytes_written` counter. The old gate
                // was a monotonic tripwire that reclamation could never clear, so a
                // long-running node permanently rejected all writes once it tripped.
                // compares live resident size and evicts; this at least lets
                // reclamation re-admit writes.
                .map(|limit| self.page_store.zone_summary().total_known_physical_bytes >= limit)
                .unwrap_or(false)
        {
            return ExecuteResponse {
                status: Status::error(
                    "storage_quota_exceeded",
                    "shard maxmemory_bytes limit has been reached",
                ),
                response: CommandResponse::Empty,
            };
        }
        // Command preconditions are a LEADER-time gate: a command only reaches the WAL after
        // passing them on the leader. WAL replay re-applies already-committed effects (like
        // ReplayWal, which does not re-check preconditions), so re-validating here
        // against reconstructed state + the restart clock is both redundant and unsafe --
        // e.g. a replayed EXPIRE whose earlier deadline has since lapsed would fail the
        // liveness precondition and abort the whole shard load. Skip validation during
        // replay, mirroring the WAL-append / index-anchor guards below.
        if !replaying_wal() {
            if let Err(status) = validate_command_preconditions(
                &self.cache,
                &self.page_store,
                request.shard_id,
                shard,
                &command,
            ) {
                return ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                };
            }
        }
        // Start this write with nothing staged, so a page put aside by a command that never
        // appended cannot ride along on the next command's record.
        if block_in_wal::enabled() {
            block_in_wal::begin_write();
        }
        let outcome = execute_on_shard(
            &self.cache,
            &self.page_store,
            config.feature_max_size,
            config.async_storage,
            config.control_rollup_enabled(),
            config.control_coalesce_persist_enabled(),
            config.control_distinct_sketch_enabled(),
            request.shard_id,
            start_routing_bucket,
            end_routing_bucket,
            shard,
            command.clone(),
        );
        // LRU recency: record that this command touched its
        // bucket(s), read or write, so eviction can prefer least-recently-used buckets.
        {
            let now = now_ms();
            for key in command_touched_keys(&command) {
                let recency_bucket =
                    page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                shard.bucket_recency.insert(recency_bucket, now);
            }
        }
        // Set to the reserved WAL sequence when the concurrent-commit path defers this
        // write's durable barrier out of the `shards` lock (TS_ENGINE_CONCURRENT_COMMIT).
        // The barrier is awaited AFTER the lock is released, just before the ack.
        let mut pending_barrier_seq: Option<u64> = None;
        if outcome.mutated {
            let object_keys = command_object_keys(&command);
            // Capture this write's touched keys for the O(delta) index-log append below
            // (the command is moved into the WAL append before we reach that point).
            let delta_command_keys = object_keys.clone();
            let upsert_components = command_upsert_components(&command);
            if object_keys.is_empty() {
                rebuild_bucket_page_ownership(
                    request.shard_id,
                    shard,
                    info.as_ref()
                        .map(|info| info.start_routing_bucket)
                        .unwrap_or_default(),
                    info.as_ref()
                        .map(|info| info.end_routing_bucket)
                        .unwrap_or(u32::MAX),
                );
            } else {
                for object_key in object_keys {
                    shard.dirty_objects.insert(object_key.clone());
                    let start_routing_bucket = info
                        .as_ref()
                        .map(|info| info.start_routing_bucket)
                        .unwrap_or_default();
                    let end_routing_bucket = info
                        .as_ref()
                        .map(|info| info.end_routing_bucket)
                        .unwrap_or(u32::MAX);
                    if config.async_storage {
                        mark_async_dirty_object(
                            shard,
                            &object_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        );
                    } else {
                        mark_async_dirty_object(
                            shard,
                            &object_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        );
                    }
                }
            }
            // Rebuild the first-index only outside the deferred-reconstruct windows
            // (bulk backfill / WAL replay). In those windows the promote step that
            // would populate bucket_map is deferred to the single reconstruct, so
            // bucket_map stays empty; the `is_empty()` clause (and the context path,
            // which never updates bucket_index directly) would then fire a full
            // O(store) rebuild on EVERY record -> O(n^2). The single reconstruct
            // rebuilds the first-index once at the end, so deferring here is
            // correctness-preserving.
            let rebuilt_bucket_index = !defer_bucket_index_reconstruct()
                && (!command_updates_bucket_index_directly(&command)
                    || shard.bucket_index.bucket_map.is_empty());
            if rebuilt_bucket_index {
                rebuild_bucket_first_index(
                    request.shard_id,
                    shard,
                    start_routing_bucket,
                    end_routing_bucket,
                );
            }
            if !defer_bucket_index_reconstruct() {
                if rebuilt_bucket_index {
                    // The rebuild replaced bucket_map wholesale, so the record of which buckets
                    // changed no longer describes it; recompute everything.
                    refresh_bucket_runtime_flags(shard);
                } else {
                    // Refresh only what this write touched. Sweeping the shard here cost
                    // O(total pages) on EVERY write, which made ingestion quadratic in the corpus.
                    refresh_pending_bucket_runtime_flags(shard);
                }
            }
            // Every
            // write records a WAL entry before any page is written.
            // async_storage only changes whether the commit BLOCKS: sync -> fsync,
            // async (or bulk backfill) -> buffered, no fsync (a fire-and-forget
            // commit). Page/index materialization stays deferred to dump.
            if write_command && !replaying_wal() {
                // A write that changed the shard and recorded nothing cannot be replaced by its
                // record. Off unless a sweep asks for it; when it is on, every existing test that
                // writes anything becomes a probe for that property -- which covers far more of
                // the mutating surface than a hand-listed fixture per command ever would.
                if crate::wal::wal_outcome_items_enabled()
                    && crate::wal::wal_outcome_strict()
                    && block_in_wal::staged_outcome_count() == 0
                {
                    let rendered = format!("{command:?}");
                    let label = rendered
                        .split_once(' ')
                        .map(|(head, _)| head.to_string())
                        .unwrap_or(rendered);
                    panic!(
                        "TS_WAL_OUTCOME_STRICT: {label} changed shard {} and recorded nothing about what it did",
                        request.shard_id
                    );
                }
                let sync = !config.async_storage && !bulk_ingest_mode();
                // Concurrent-commit path (gated, default OFF): for a synchronous write, only
                // RESERVE the WAL sequence + append the bytes here (under the `shards` lock);
                // the durable fdatasync barrier is deferred to `commit_barrier` AFTER the lock
                // is released, so concurrent same-shard writers reach the group-commit queue in
                // parallel and coalesce their fsyncs (see wal.rs::group_commit_sync). WAL
                // sequence order still equals in-memory apply order because the reservation +
                // byte-append stay under this same lock, exactly as append_with_sync did; only
                // the order-independent fsync moves out. Off -> byte-identical append_with_sync.
                // In a raft apply batch (TS_RAFT_APPLY_COALESCE) reuse the same reserve-only
                // append: each committed entry appends its bytes with sync=false and RESERVES its
                // WAL sequence here; the single coalesced fdatasync is issued once for the whole
                // batch in `execute_raft_apply_batch` (see `raft_apply_batch_active`). WAL order
                // still equals apply order (reservation + byte-append stay under this lock).
                // The reserve-only branch appends bytes without pages, so a write carrying
                // pages must take the staged branch or they would be dropped on the floor.
                // Staged pages still force the other branch -- their addresses are back-patched
                // once the record's log id exists, which this path does not do. Outcomes no
                // longer do: they are resolved before they are staged, so they ride along and a
                // recording write keeps its place in the group-commit queue.
                let concurrent_commit = sync
                    && carried_pages.is_empty()
                    && (engine_concurrent_commit() || raft_apply_batch_active());
                // Where each page this write stages ends up, so the index can carry it. Filled
                // by the append below, which is the first moment the log id exists.
                let mut wal_resident_updates: Vec<(u64, crate::engine::state::WalResidentPage)> =
                    Vec::new();
                let append_result = if concurrent_commit {
                    self.wal_store
                        .append_for_group_commit(
                            request.shard_id,
                            command,
                            if crate::wal::wal_outcome_items_enabled() {
                                block_in_wal::take_outcomes()
                            } else {
                                Vec::new()
                            },
                        )
                        .map(|record| Some(record.sequence))
                } else {
                    self.wal_store
                        .append_with_outcomes(
                            request.shard_id,
                            command,
                            sync,
                            if carried_pages.is_empty() {
                                if block_in_wal::enabled() {
                                    block_in_wal::take_staged()
                                } else {
                                    Vec::new()
                                }
                            } else {
                                // The caller handed us the pages the original write produced.
                                // Drop whatever this execute re-derived rather than letting a
                                // reconstruction win over the bytes that were acked.
                                if block_in_wal::enabled() {
                                    let _ = block_in_wal::take_staged();
                                }
                                std::mem::take(&mut carried_pages)
                            },
                            if crate::wal::wal_outcome_items_enabled() {
                                block_in_wal::take_outcomes()
                            } else {
                                Vec::new()
                            },
                        )
                        .map(|(record, log_id)| {
                            // Point every page this record carries at the record, keyed on the
                            // object id the write derived -- which is what the stored address
                            // carries, so a read finds it by identity rather than by timing.
                            if block_in_wal::enabled() {
                                block_in_wal::register_record(
                                    &self.page_store,
                                    request.shard_id,
                                    &record.staged_pages,
                                    log_id,
                                    record.sequence,
                                    &self.wal_store,
                                );
                                // Same fact, written down where it survives this process.
                                wal_resident_updates.extend(record.staged_pages.iter().map(
                                    |page| {
                                        (
                                            page.object_id,
                                            crate::engine::state::WalResidentPage {
                                                log_id,
                                                sequence: record.sequence,
                                            },
                                        )
                                    },
                                ));
                            }
                            None
                        })
                };
                match append_result {
                    Ok(deferred_seq) => {
                        // The index carries where each staged page landed, so a reload can hand
                        // the mapping back rather than leaving the address unresolvable until a
                        // full replay re-derives the page.
                        for (object_id, placement) in wal_resident_updates.drain(..) {
                            shard.wal_resident_pages.insert(object_id, placement);
                        }
                        // In concurrent-commit mode remember the reserved sequence; its durable
                        // barrier is awaited after the `shards` lock is dropped (below). The ack
                        // is returned strictly AFTER that barrier succeeds -- never before.
                        pending_barrier_seq = deferred_seq;
                    }
                    Err(err) => {
                    if sync {
                        // A synchronous write whose durable WAL commit failed is NOT durable: the
                        // WAL is the recovery source of truth (replayed on load), so returning ok
                        // would tell the client a write that is gone after a crash succeeded.
                        // surfaces the wal Commit failure to the client
                        // The failed commit status is copied into the response.
                        // rather than acking a non-durable write. Match that instead of swallowing
                        // the error. (async/bulk mode is a fire-and-forget commit, so
                        // its append errors stay
                        // best-effort and do not fail the command.) We also skip the index anchor
                        // + persist below, so durable state never advances past a write the WAL
                        // did not accept.
                        return ExecuteResponse {
                            status: Status::error(
                                "wal_commit_failed",
                                format!("durable WAL commit failed: {err}"),
                            ),
                            response: CommandResponse::Empty,
                        };
                    } else {
                        // Async / bulk mode is a fire-and-forget commit, so an append error does
                        // NOT fail the command (the ack path is intentionally best-effort here).
                        // But it must never be swallowed silently: a dropped async append means
                        // the recovery source of truth is missing this write, so surface it in the
                        // logs (with the failing shard) so operators can see acked-but-undurable
                        // writes instead of discovering them only as post-crash data loss.
                        tracing::error!(
                            shard_id = request.shard_id,
                            error = %err,
                            "async WAL append failed: write acked to the client is NOT durable \
                             and will be lost on a crash before the next flush"
                        );
                    }
                    }
                }
            }
            if !config.async_storage && !bulk_ingest_mode() && !replaying_wal() {
                // Anchor the (in-memory) served index to the WAL sequence it now reflects, so a
                // later load replays only records written after this point (the
                // dumped-log-id anchor read back on load). Reading the sequence via `stats()`
                // triggers a full-file `last_wal_sequence_at` rescan -- an O(records)-per-write cost
                // under this lock (stack-sampling shows it dominates a warm ingest). Under
                // TS_PHASE1_FLAT anchor off the O(1) cached last sequence (authoritative right after
                // this write's append) instead; gate OFF keeps the exact `stats()` value.
                shard.applied_wal_sequence = Some(if phase1_flat_enabled() || raft_apply_batch_active() {
                    self.wal_store.cached_last_sequence(request.shard_id)
                } else {
                    self.wal_store.stats(request.shard_id).last_sequence
                });
                // Append ONLY the pages this write changed (O(delta)) to the index-log,
                // advancing the index-log sequence and populating the served-index delta
                // stream. The whole base index is NOT rewritten per write (that O(store) path
                // is gone); the base is materialized at compaction/unload, the funnel serves
                // the live in-memory shard between them, and cold reload folds base + deltas.
                let (items, upsert_record) = match upsert_components
                    .as_ref()
                    .filter(|_| upsert_deltas_enabled())
                {
                    Some(components) => (
                        collect_upsert_index_items(
                            shard,
                            request.shard_id,
                            components,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                        true,
                    ),
                    None => (
                        collect_command_index_items(
                            shard,
                            &delta_command_keys,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                        false,
                    ),
                };
                let key_states = capture_key_states(shard, &delta_command_keys);
                // `durable` fsyncs the delta record before returning. Deferred on the raft
                // apply path (raft log is the durability source) and, under the single-barrier
                // default, on the single-node path too: the record is still written (so the
                // served-index stream is unchanged), but the durable WAL barrier already
                // committed above makes the lost delta tail recoverable by base-only WAL replay,
                // so its fdatasync leaves the ack critical path. Restored to a synchronous
                // barrier only under the TS_WAL_LEGACY_RECOVERY escape hatch (wal_single_barrier
                // false -> delta-fold recovery, which trusts the durable delta).
                let index_log_durable = !raft_applying() && !wal_single_barrier();
                let _ = self.index_log_store.append_delta(
                    request.shard_id,
                    items,
                    key_states,
                    shard.applied_wal_sequence,
                    None,
                    upsert_record,
                    index_log_durable,
                );
            }
        }
        // Release the `shards` write lock BEFORE the durable barrier. A concurrent same-shard
        // writer can now acquire it, mutate + reserve its own WAL sequence, and enter the
        // group-commit queue WHILE this writer's fdatasync is in flight -- the coalescing window
        // that makes group commit engage (fewer fsyncs than writes). The in-memory mutation and
        // WAL sequence reservation already completed under the lock above, so WAL order == apply
        // order holds regardless of how the (order-independent) barriers interleave. When the
        // concurrent-commit gate is OFF, `pending_barrier_seq` is None and the barrier below is a
        // no-op, so this is byte-identical to the prior in-lock append_with_sync path.
        drop(shards);
        if let Some(barrier_seq) = pending_barrier_seq {
            if raft_apply_batch_active() {
                // Defer to the single coalesced barrier issued for the whole batch by
                // `execute_raft_apply_batch` (the record bytes are already reserved + buffered).
                record_raft_apply_batch_barrier(request.shard_id, barrier_seq);
            } else if let Err(err) = self.wal_store.commit_barrier(request.shard_id, barrier_seq) {
                // The coalesced durable barrier failed: this synchronous write is NOT durable, so
                // surface the failure instead of acking (mirrors the append_with_sync sync-failure
                // path -- the ack is returned strictly after a successful barrier, never before).
                return ExecuteResponse {
                    status: Status::error(
                        "wal_commit_failed",
                        format!("durable WAL commit barrier failed: {err}"),
                    ),
                    response: CommandResponse::Empty,
                };
            }
        }
        ExecuteResponse {
            status: Status::ok(),
            response: outcome.response,
        }
    }

    fn execute_read_only_fast_path(&self, request: &ExecuteRequest) -> Option<ExecuteResponse> {
        let read_command = matches!(
            request.command,
            Command::StringGet { .. } | Command::HashGetAll { .. }
        );
        if !read_command {
            return None;
        }
        // A shard replaying its WAL on load is present but not yet serving (keeps it in
        // PartitionLoadStage::LOADING). The durable / replicated read routes reach this fast
        // path BEFORE execute_with_storage_override's recovering gate, so without this a read
        // would be served from half-reconstructed state (and skip admission). Decline the fast
        // path while recovering so the slow path rejects uniformly with a retryable
        // shard_not_loaded. The replay thread reads under replaying_wal(), which bypasses this.
        if !replaying_wal()
            && self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&request.shard_id)
                .map(|info| info.recovering)
                .unwrap_or(false)
        {
            return None;
        }
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&request.shard_id) else {
            return Some(ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            });
        };
        match &request.command {
            Command::StringGet { key } => {
                if shard
                    .expires_at_ms
                    .get(key)
                    .map(|expires_at| *expires_at <= now_ms())
                    .unwrap_or(false)
                {
                    return None;
                }
                Some(ExecuteResponse {
                    status: Status::ok(),
                    response: cached_response(
                        &self.cache,
                        CacheKey::string(request.shard_id, key),
                        || CommandResponse::Bytes {
                            value: shard.strings.get(key).and_then(|address| {
                                read_page_bytes(
                                    &self.cache,
                                    &self.page_store,
                                    request.shard_id,
                                    address,
                                )
                            }),
                        },
                    ),
                })
            }
            Command::HashGetAll { key } => {
                if shard
                    .expires_at_ms
                    .get(key)
                    .map(|expires_at| *expires_at <= now_ms())
                    .unwrap_or(false)
                {
                    return None;
                }
                let entries = shard
                    .hashes
                    .get(key)
                    .map(|fields| {
                        let mut entries = fields
                            .iter()
                            .filter_map(|(field, address)| {
                                read_page_bytes(
                                    &self.cache,
                                    &self.page_store,
                                    request.shard_id,
                                    address,
                                )
                                .map(|value| (field.clone(), value))
                            })
                            .collect::<Vec<_>>();
                        entries.sort_by(|a, b| a.0.cmp(&b.0));
                        entries
                    })
                    .unwrap_or_default();
                Some(ExecuteResponse {
                    status: Status::ok(),
                    response: CommandResponse::HashEntries { entries },
                })
            }
            _ => None,
        }
    }

    pub fn execute_checked(&self, request: CheckedExecuteRequest) -> CheckedExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            };
        }
        let response = self.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        });
        CheckedExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    fn check_admission(
        &self,
        shard_id: ShardId,
        write_command: bool,
        config: &Config,
        info: &Option<ShardInfo>,
    ) -> Result<(), Status> {
        let limits = admission_limits(shard_id, write_command, config, info);
        if limits.is_empty() {
            return Ok(());
        }
        let now_sec = now_epoch_seconds();
        let mut admissions = self.admissions.write().expect("admission lock poisoned");
        for limit in &limits {
            if limit.limit == 0 {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} is zero", limit.label),
                ));
            }
            let admission = admissions.entry(limit.scope.clone()).or_default();
            reset_admission_window(admission, now_sec);
            let count = admission_count(admission, write_command);
            if *count >= limit.limit {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} limit exceeded", limit.label),
                ));
            }
        }
        for limit in limits {
            let admission = admissions.entry(limit.scope).or_default();
            reset_admission_window(admission, now_sec);
            *admission_count(admission, write_command) += 1;
        }
        Ok(())
    }

    pub fn set_config(&self, request: SetConfigRequest) -> Status {
        if !self.is_shard_loaded(request.shard_id) {
            return Status::error("shard_not_found", "shard is not loaded");
        }
        let mut configs = self.configs.write().expect("config lock poisoned");
        let current = configs.get(&request.shard_id).cloned().unwrap_or_default();
        if request.config.version < current.version {
            return Status::error("failed_precondition", "legacy config version");
        }
        if request.config.version == current.version {
            return Status::ok();
        }
        let applied = request.config.clone();
        configs.insert(request.shard_id, request.config);
        // Drop the config lock before touching the WAL/disk so the durable append never runs
        // under the config mutex.
        drop(configs);
        // Durably log the config so it survives reload REGARDLESS of barrier mode. Runtime config
        // (feature_max_size + the representation-changing extend gate flags: control_rollup /
        // coalesce / distinct_sketch) is NOT carried in the served-index checkpoint, so without a
        // durable config-log a reload defaults `Config` and silently resets these -- which for the
        // representation flags can misread already-written data. Stamp the change with the current
        // WAL frontier (effective for every write with a strictly greater sequence) and fsync it.
        // Config changes are rare admin ops, so this barrier is off the per-write hot path. In
        // single-barrier mode WAL-tail replay additionally re-derives config-driven trims at this
        // exact frontier; in every other mode the last entry is simply restored as the live config
        // on load (see load_shard_with / replay_wal_into_shard).
        let after_seq = self.wal_store.stats(request.shard_id).last_sequence;
        if let Err(err) = self.append_config_log_entry(request.shard_id, after_seq, &applied) {
            tracing::warn!(
                shard_id = request.shard_id,
                error = %err,
                "failed to persist config-log entry"
            );
        }
        Status::ok()
    }

    /// Durable, WAL-sequence-ordered config-log path for a shard (single-barrier mode).
    pub(super) fn config_log_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir
            .join(format!("shard-{shard_id}.configlog.jsonl"))
    }

    /// Append one config-log entry `{after_seq, config}` and fsync it. `after_seq` is the WAL
    /// sequence the config became effective AFTER (it applies to writes with sequence >
    /// after_seq). Append-only + fsync'd so a crash cannot lose an acked config change.
    pub(super) fn append_config_log_entry(
        &self,
        shard_id: ShardId,
        after_seq: u64,
        config: &Config,
    ) -> std::io::Result<()> {
        use std::io::Write as _;
        std::fs::create_dir_all(&self.index_dir)?;
        let entry = ConfigLogEntry {
            after_seq,
            config: config.clone(),
        };
        let mut bytes = serde_json::to_vec(&entry)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        bytes.push(b'\n');
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.config_log_path(shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        file.sync_data()?;
        Ok(())
    }

    /// Read the config-log entries for a shard, sorted by ascending `after_seq` (stable). Empty
    /// when the shard has no config-log (no single-barrier config change was ever persisted).
    pub(super) fn config_log_entries(&self, shard_id: ShardId) -> Vec<ConfigLogEntry> {
        let path = self.config_log_path(shard_id);
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(_) => return Vec::new(),
        };
        let mut entries: Vec<ConfigLogEntry> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<ConfigLogEntry>(line).ok())
            .collect();
        entries.sort_by_key(|entry| entry.after_seq);
        entries
    }

    pub fn get_config(&self, shard_id: ShardId) -> GetConfigResponse {
        if !self.is_shard_loaded(shard_id) {
            return GetConfigResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
                config: Config::default(),
            };
        }
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();
        GetConfigResponse {
            status: Status::ok(),
            config,
        }
    }

    fn is_shard_loaded(&self, shard_id: ShardId) -> bool {
        self.infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
    }

    pub fn get_info(&self, shard_id: ShardId) -> GetInfoResponse {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        GetInfoResponse {
            status: if info.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            info,
        }
    }

    pub fn update_membership(&self, request: MembershipUpdateRequest) -> Status {
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            if request.membership_version < info.membership_version {
                return Status::error("failed_precondition", "legacy membership info");
            }
            let global_update = request.membership_version > info.membership_version;
            if !global_update
                && request.replica_membership_version == info.replica_membership_version
            {
                return Status::ok();
            }
            if request.replica_membership_version < info.replica_membership_version {
                return Status::error("failed_precondition", "legacy membership unit info");
            }
            info.replica_node_ids = request.replica_node_ids;
            info.leader_node_id = request.leader_node_id;
            info.membership_version = request.membership_version;
            info.replica_membership_version = request.replica_membership_version;
            info.membership_valid = info
                .local_node_id
                .map(|node_id| info.replica_node_ids.contains(&node_id))
                .unwrap_or(true);
            Status::ok()
        } else {
            Status::error("shard_not_found", "shard is not loaded")
        }
    }

    pub fn get_stats(&self, shard_id: ShardId) -> GetStatsResponse {
        let stats = self.shard_stats(shard_id);
        GetStatsResponse {
            status: if stats.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            stats,
        }
    }

    pub fn rust_storage_observation(&self, shard_id: ShardId) -> Option<RustStorageObservation> {
        self.shard_stats(shard_id)
            .map(|stats| RustStorageObservation {
                shard_id,
                observed_memory_hit: stats.cache.memory_hits > 0,
                observed_block_cache_hit: stats.cache.disk_hits > 0,
                observed_local_file_read: stats.page_store.reads > 0,
                observed_cache_invalidation: stats.cache.invalidations > 0,
                observed_memory_eviction: stats.cache.memory_evictions > 0,
                cache_memory_bytes: stats.cache.memory_bytes,
                cache_disk_bytes: stats.cache.disk_bytes,
                local_page_bytes_written: stats.page_store.bytes_written,
                local_page_bytes_read: stats.page_store.bytes_read,
                cache: stats.cache,
                page_store: stats.page_store,
            })
    }

    #[doc(hidden)]
    pub fn string_page_cache_key_for_test(&self, shard_id: ShardId, key: &str) -> Option<CacheKey> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let address = shards.get(&shard_id)?.strings.get(key)?;
        Some(CacheKey::page_with_slot(
            shard_id,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket))
    }

    #[doc(hidden)]
    pub fn clear_string_model_view_for_test(&self, shard_id: ShardId, key: &str) -> bool {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        shards
            .get_mut(&shard_id)
            .and_then(|shard| shard.strings.remove(key))
            .is_some()
    }

    pub fn loaded_shard_stats(&self) -> Vec<ShardStats> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.shard_stats(shard_id))
            .collect()
    }

    pub fn loaded_shard_ids(&self) -> Vec<ShardId> {
        let mut shard_ids = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids
    }

    pub fn bucket_storage_summaries(&self, shard_id: ShardId) -> Vec<BucketStorageSummary> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        let summaries = bucket_storage_summaries(shard, start, end);
        if let Some(manifest) = latest_bucket_dump_manifest_at(&self.index_dir, shard_id) {
            merge_last_dump_sequence(summaries, &manifest)
        } else {
            summaries
        }
    }

    pub fn storage_physical_index_report(&self, shard_id: ShardId) -> StoragePhysicalIndexReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return StoragePhysicalIndexReport {
                shard_id,
                bucket_first: true,
                ..StoragePhysicalIndexReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        let summaries = bucket_storage_summaries(shard, start, end);
        let summaries =
            if let Some(manifest) = latest_bucket_dump_manifest_at(&self.index_dir, shard_id) {
                merge_last_dump_sequence(summaries, &manifest)
            } else {
                summaries
            };
        storage_physical_index_report(shard_id, shard, summaries)
    }

    pub fn bucket_object_page_ownership_report(
        &self,
        shard_id: ShardId,
    ) -> BucketObjectPageOwnershipReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return BucketObjectPageOwnershipReport {
                shard_id,
                ..BucketObjectPageOwnershipReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        bucket_object_page_ownership_report(
            shard_id,
            shard,
            info.as_ref()
                .map(|info| info.start_routing_bucket)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_bucket)
                .unwrap_or(u32::MAX),
        )
    }

    pub fn object_manager_runtime_report(&self, shard_id: ShardId) -> ObjectManagerRuntimeReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return ObjectManagerRuntimeReport {
                shard_id,
                blockers: vec!["shard is not loaded".to_string()],
                ..ObjectManagerRuntimeReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        object_manager_runtime_report(
            shard_id,
            shard,
            info.as_ref()
                .map(|info| info.start_routing_bucket)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_bucket)
                .unwrap_or(u32::MAX),
        )
    }

    pub fn storage_data_structure_api_parity_report(
        &self,
        shard_id: ShardId,
    ) -> StorageDataStructureApiParityReport {
        let physical_index = self.storage_physical_index_report(shard_id);
        let ownership = self.bucket_object_page_ownership_report(shard_id);
        let object_manager = self.object_manager_runtime_report(shard_id);
        let slab_reports = self.page_store.slab_reports().unwrap_or_default();
        let block_index_count = slab_reports
            .iter()
            .map(|slab| slab.block_index_count)
            .sum::<u64>();
        let block_address_api_ready = slab_reports.iter().any(|slab| {
            slab.block_index_entries.iter().any(|entry| {
                entry.compact_slab_address.is_some()
                    && entry.compact_slab_id.is_some()
                    && entry.compact_slab_offset.is_some()
                    && entry.block_id.is_some()
                    && entry.object_id.is_some()
                    && entry.routing_bucket.is_some()
                    && entry.checksum.is_some()
            })
        });
        let band_report = self.page_store.stream_backed_band_runtime_report().ok();
        let stream_backed_band_api_ready = band_report
            .as_ref()
            .map(|report| {
                report.band_manifest_ready
                    && report.band_manifest_disk_consistent
                    && report.zone_stats_ready
                    && report.stream_record_count > 0
                    && report.blockers.iter().all(|blocker| {
                        blocker.contains("append/roll") || blocker.contains("purge lifecycle")
                    })
            })
            .unwrap_or(false);
        let storage_manager = self.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id,
            dry_run: true,
            ..StorageManagerCycleRequest::default()
        });
        let expected_stages = [
            "prepare",
            "reclaim_wal",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ];
        let storage_manager_phase_api_ready = storage_manager.completed
            && expected_stages.iter().all(|stage| {
                storage_manager
                    .native_stage_order
                    .iter()
                    .any(|observed| observed == stage)
                    && storage_manager
                        .stages
                        .iter()
                        .any(|observed| observed.stage == *stage)
            });
        let storage_manager_pressure_api_ready =
            storage_manager.pressure_signals.total_pressure_score
                >= storage_manager.pressure_signals.dirty_bucket_count as u64
                && storage_manager
                    .stages
                    .iter()
                    .any(|stage| stage.pressure_triggered || stage.pressure_score > 0);
        let storage_manager_merged_dump_load_api_ready =
            storage_manager.merged_dump_load_policy.policy_ready
                || storage_manager
                    .merged_dump_load_policy
                    .blockers
                    .iter()
                    .all(|blocker| blocker.contains("no dirty slots"));
        let bucket_store_layout_api_ready = physical_index.bucket_nodes.iter().any(|bucket| {
            matches!(
                bucket.layout.as_str(),
                "single_object" | "single_page_object" | "multi_page_object" | "multi_object"
            )
        });
        let mut blockers = Vec::new();
        if !physical_index.bucket_index_authority || !ownership.first_class_index_present {
            blockers.push("slot_object_page_authority_missing".to_string());
        }
        if !bucket_store_layout_api_ready {
            blockers.push("slot_store_layout_states_missing".to_string());
        }
        if !object_manager.runtime_ready {
            blockers.push("object_manager_runtime_not_ready".to_string());
        }
        if !block_address_api_ready {
            blockers.push("block_address_metadata_incomplete".to_string());
        }
        if block_index_count == 0 {
            blockers.push("block_store_segment_index_missing".to_string());
        }
        if !stream_backed_band_api_ready {
            blockers.push("stream_backed_band_api_not_ready".to_string());
        }
        if !storage_manager_phase_api_ready {
            blockers.push("storage_manager_phase_api_incomplete".to_string());
        }
        if !storage_manager_pressure_api_ready {
            blockers.push("storage_manager_pressure_api_incomplete".to_string());
        }
        if !storage_manager_merged_dump_load_api_ready {
            blockers.push("storage_manager_merged_dump_load_api_incomplete".to_string());
        }
        let legacy_page_zone_aliases_ready = true;
        StorageDataStructureApiParityReport {
            shard_id,
            ready: blockers.is_empty() && legacy_page_zone_aliases_ready,
            bucket_object_page_authority_ready: physical_index.bucket_index_authority
                && ownership.first_class_index_present
                && !ownership.derived_from_model_maps,
            bucket_store_layout_api_ready,
            object_manager_runtime_api_ready: object_manager.runtime_ready,
            block_address_api_ready,
            block_store_slab_api_ready: block_index_count > 0,
            stream_backed_band_api_ready,
            legacy_page_zone_aliases_ready,
            storage_manager_phase_api_ready,
            storage_manager_pressure_api_ready,
            storage_manager_merged_dump_load_api_ready,
            bucket_count: physical_index.bucket_count,
            page_index_count: physical_index.page_index_count,
            block_index_count,
            stream_band_count: band_report
                .as_ref()
                .map(|report| report.band_count)
                .unwrap_or_default(),
            stream_record_count: band_report
                .as_ref()
                .map(|report| report.stream_record_count)
                .unwrap_or_default(),
            storage_manager_stage_order: storage_manager.native_stage_order,
            blockers,
            evidence: vec![
                "slot/object/page authority is reported from the first-class slot index"
                    .to_string(),
                "block addresses expose segment, offset, length, block id, object id, routing slot, band id, and checksum"
                    .to_string(),
                "stream-backed storage exposes active/sealed/delayed-destroy/purged band lifecycle while accepting legacy zone aliases"
                    .to_string(),
                "StorageManager exposes standard prepare/reclaim/expire/evict/reclaim-page/index-GC/compact/reap-metrics phases"
                    .to_string(),
            ],
        }
    }

    pub fn routing_bucket_for_key(&self, shard_id: ShardId, key: &str) -> u32 {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        page_routing_bucket(key, start, end)
    }

}

/// Inclusive `[start, end]` timestamp bounds for `BTreeMap::range` that yield an EMPTY range
/// (never panic) when `start > end`. `BTreeMap::range` panics on reversed bounds, and every
/// range query runs under the shard write lock, so a client sending `start_ms > end_ms` would
/// poison the lock and take the whole shard down (every later `.lock().expect()` panics).
/// `RangeGet` simply iterates and returns an empty result with Status::OK when `min > max`
/// so match that: reversed bounds → empty range, not a crash. For
/// `start <= end` this is byte-for-byte the same set as `start..=end`.
pub(crate) fn timestamp_range_bounds(
    start: u64,
    end: u64,
) -> (std::ops::Bound<u64>, std::ops::Bound<u64>) {
    use std::ops::Bound;
    if start <= end {
        (Bound::Included(start), Bound::Included(end))
    } else {
        // Empty, non-panicking: `[1, 1)` contains nothing and is a valid (not both-excluded) range.
        (Bound::Included(1), Bound::Excluded(1))
    }
}

fn bulk_ingest_mode() -> bool {
    matches!(
        std::env::var("MATRIXARK_BULK_INGEST")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether the per-command model-map -> bucket-index promotion and first-index
/// rebuild should be DEFERRED to a single reconstruct pass. True during bulk
/// backfill (MATRIXARK_BULK_INGEST) and during WAL replay on load: both re-drive
/// many already-committed writes with no interleaved client reads, so running the
/// O(store) promote/rebuild per command is the dominant O(n^2) cost. Deferring is
/// lossless because point string/hash/set reads+writes maintain the bucket_map /
/// object_page_lookup directly via upsert_bucket_index_page/read_bucket_index_value,
/// and the deferred context (model-map) records are append-only until the single
/// reconstruct folds them in (bulk: flush_shard_index(); replay: replay_wal_into_shard()).
fn defer_bucket_index_reconstruct() -> bool {
    bulk_ingest_mode() || replaying_wal()
}

/// Whether load_shard should eagerly warm the in-memory cache tier from the page
/// store after reconstructing the index (disk->memory promotion on restart).
/// Defaults ON; set MATRIXARK_EAGER_CACHE_WARM_ON_LOAD to 0/false/off/no to disable.
fn eager_cache_warm_on_load() -> bool {
    !matches!(
        std::env::var("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

/// Current on-disk shape of a shard index.
///
/// 1 = pre-rekey: context_events keyed by timeline_key.
/// 2 = context_events keyed by event_id_hash, with context_event_timeline carrying time order.
///
/// Bump this whenever a field's MEANING changes, not only when its type does -- a same-typed
/// reinterpretation is the case that decodes cleanly and serves wrong data.
pub(super) const SHARD_INDEX_FORMAT_VERSION: u32 = 2;

/// Serialize a shard index, stamping the current format version.
///
/// Stamped here rather than held on the struct so ShardState keeps its derived Default: an
/// in-memory shard would otherwise default the field to 0 and write itself out looking legacy.
pub(super) fn stamp_index_format_version(shard: &ShardState) -> serde_json::Value {
    let mut value = serde_json::to_value(shard).expect("shard index should serialize");
    if let Some(map) = value.as_object_mut() {
        map.insert(
            "index_format_version".to_string(),
            serde_json::Value::from(SHARD_INDEX_FORMAT_VERSION),
        );
    }
    value
}

/// Serialize a shard whose stamp is already current, straight to bytes.
///
/// The stamping path builds an entire intermediate `serde_json::Value` tree of the whole index
/// before encoding it -- serializing a multi-megabyte structure twice, once into a tree of
/// allocations and once into bytes, on every snapshot. A shard whose version field is already
/// correct needs none of that; one whose field is stale still takes the stamping path, so the
/// bytes are identical either way.
pub(super) fn serialize_index_stamped(shard: &mut ShardState) -> Vec<u8> {
    shard.index_format_version = SHARD_INDEX_FORMAT_VERSION;
    encode_index_bytes(shard)
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    if shard.index_format_version == SHARD_INDEX_FORMAT_VERSION {
        return encode_index_bytes(shard);
    }
    wrap_index_json(
        serde_json::to_vec(&stamp_index_format_version(shard))
            .expect("shard index should serialize"),
    )
}

/// Container magic for a non-JSON served index. A JSON index starts with `{`, so a reader can
/// tell the two apart from the first byte and never has to be told which it is holding.
const INDEX_CONTAINER_MAGIC: &[u8] = b"TSIDX\x01";

/// Payload codec ids inside the container. The whole point of the container is that this is an
/// enumeration rather than a decision: each payload is an id whose decoder lands beside the
/// others, and every index written before it keeps loading.
const INDEX_CODEC_ZSTD_JSON: u8 = 1;
/// The struct's own serde model in a binary encoding, then compressed.
///
/// Two decisions are baked in here, and measurement forced both.
///
/// NOT a hand-written protobuf schema. The served index IS a `ShardState` and has to round-trip
/// one exactly. A schema mirroring its ~26 fields and their nested maps is a second definition of
/// the same type, and the two drifting apart fails SILENTLY -- a field added here and forgotten
/// there simply disappears from the durable image, and the loss surfaces as missing data after a
/// reload. Riding the existing derives means a new field participates because it exists, not
/// because someone remembered to add it in two places.
///
/// NOT a field-ORDER encoding either, which is what an earlier attempt at this used. These structs
/// lean on `#[serde(skip_serializing_if)]` and `#[serde(default)]` -- `BlockAddress` alone skips
/// six optional fields -- so the writer omits fields that a positional decoder still expects and
/// the stream slides out of alignment. Tried directly here: the round-trip fails with "tag for
/// enum is not valid, found 9". A self-describing encoding (field names, struct-as-map) keeps
/// exactly the semantics JSON had, which is the only way those attributes stay honest.
const INDEX_CODEC_ZSTD_MSGPACK: u8 = 2;

/// Binary payloads carry the struct version they were written from, big-endian, right after the
/// codec id. A name-free encoding read against a different struct does not fail, it MIS-READS --
/// so the version is checked before a byte is decoded, and a mismatch is refused. This is also the
/// trap that sank the previous attempt from the other end: the struct's own
/// `index_format_version` field lives INSIDE the payload, so it cannot be consulted until after
/// the decode it is supposed to guard, and a fresh shard carries 0 there regardless.
const INDEX_BINARY_VERSION_BYTES: usize = 4;

/// Compression level for the container payload. The served index is written whole, in the
/// background, and read whole -- so this trades a little CPU on a path that is not the request
/// path for a large cut in bytes written and bytes read at load.
const INDEX_ZSTD_LEVEL: i32 = 3;

/// TS_INDEX_BINARY: write the served index in the binary container instead of raw JSON.
/// Reading is unconditional and sniffed, so this flag only ever controls what is WRITTEN, and an
/// index written either way loads in either setting.
/// Does this look like a served index, in either of the two formats a reader may be handed?
///
/// A JSON index starts with `{`; a container starts with its magic. Callers that only need to
/// know "these bytes are an index" -- rather than to decode one -- ask this instead of parsing.
pub(crate) fn bytes_look_like_served_index(bytes: &[u8]) -> bool {
    bytes.first() == Some(&b'{') || bytes.starts_with(INDEX_CONTAINER_MAGIC)
}

/// ON by default; `TS_INDEX_BINARY=0` is the escape hatch.
///
/// The container was built, measured and then left switched off, so every store written since has
/// carried a plain-JSON served index. Measured at 300 adds into one subject, which is the shape
/// that grows an index rather than merely touching it:
///
/// ```text
///                    index      WAL    durable per memory   add p50
///     JSON          19.9 MB  15.4 MB          227.0 KB      383.1 ms
///     container      2.2 MB   2.3 MB          122.0 KB      367.8 ms
/// ```
///
/// 46% less durable disk per memory. The WAL falls with it because index deltas ride the WAL, and
/// page bytes do not move at all -- the data is unchanged, only the way the index is written.
///
/// A format default is a durability decision, not a size one, so the flip is gated on recovery
/// rather than on the table above. Three cases, 120 memories each, comparing full retrieval
/// snapshots across a restart:
///
///   * written by the container, reopened by it -- identical.
///   * written as JSON, reopened with the container on -- identical, and the index on disk becomes
///     a container, so an existing store upgrades in place with no migration step.
///   * written by the container, reopened with the hatch pulled -- identical, and the index goes
///     back to JSON. The flip is reversible in both directions, which is what makes it safe to
///     make it the default rather than an opt-in.
///
/// A reader never has to be told which it is holding: JSON starts with `{`, a container with its
/// magic, so both formats stay loadable whichever way this flag points.
fn index_binary_container_enabled() -> bool {
    env_flag_default_on("TS_INDEX_BINARY")
}

/// TS_INDEX_CODEC: which payload to write when the container is on. `msgpack` (the default when
/// the container is enabled) encodes the struct itself; `zstd-json` keeps the compressed-JSON
/// payload, which any reader can still inspect with a decompressor and a JSON parser.
fn index_container_codec() -> u8 {
    match std::env::var("TS_INDEX_CODEC")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "zstd-json" | "json" => INDEX_CODEC_ZSTD_JSON,
        _ => INDEX_CODEC_ZSTD_MSGPACK,
    }
}

/// Encode a shard with the container's binary payload: the serde model as a binary map,
/// compressed, behind the magic, the codec id and the struct version.
fn encode_index_msgpack(shard: &ShardState) -> Option<Vec<u8>> {
    // struct-as-MAP, not struct-as-array: the array form is positional, and positional is what
    // mis-reads a struct that skipped an absent optional.
    let mut encoded = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut encoded).with_struct_map();
    serde::Serialize::serialize(shard, &mut serializer).ok()?;
    let payload = zstd::stream::encode_all(encoded.as_slice(), INDEX_ZSTD_LEVEL).ok()?;
    let mut out = Vec::with_capacity(
        payload.len() + INDEX_CONTAINER_MAGIC.len() + 1 + INDEX_BINARY_VERSION_BYTES,
    );
    out.extend_from_slice(INDEX_CONTAINER_MAGIC);
    out.push(INDEX_CODEC_ZSTD_MSGPACK);
    out.extend_from_slice(&SHARD_INDEX_FORMAT_VERSION.to_be_bytes());
    out.extend_from_slice(&payload);
    Some(out)
}

/// The single place the served index becomes bytes.
///
/// The measured cost of raw JSON here is real: a 1 000-memory store carries a 74 MB index, and a
/// dump rewrites it whole. Compressing the same JSON keeps ONE representation of the struct --
/// no schema mirrored by hand, no second definition to drift -- while cutting what is written and
/// what must be read back at load.
pub(super) fn encode_index_bytes(shard: &ShardState) -> Vec<u8> {
    if index_binary_container_enabled() && index_container_codec() == INDEX_CODEC_ZSTD_MSGPACK {
        // A binary payload only works from the struct itself, so the version-stamping path (which
        // patches a `Value`) keeps to JSON; both still land inside the same container.
        if let Some(encoded) = encode_index_msgpack(shard) {
            return encoded;
        }
    }
    wrap_index_json(serde_json::to_vec(shard).expect("shard index should serialize"))
}

/// Put already-serialized index JSON into the container (or leave it as JSON when the container
/// is off). Separate from `encode_index_bytes` because the version-stamping path serializes a
/// patched `Value` rather than the struct, and both must produce the same on-disk shape.
fn wrap_index_json(json: Vec<u8>) -> Vec<u8> {
    if !index_binary_container_enabled() {
        return json;
    }
    match zstd::stream::encode_all(json.as_slice(), INDEX_ZSTD_LEVEL) {
        Ok(payload) => {
            let mut out = Vec::with_capacity(payload.len() + INDEX_CONTAINER_MAGIC.len() + 1);
            out.extend_from_slice(INDEX_CONTAINER_MAGIC);
            out.push(INDEX_CODEC_ZSTD_JSON);
            out.extend_from_slice(&payload);
            out
        }
        // A compression failure must not cost the index: fall back to the bytes that always work.
        Err(_) => json,
    }
}

/// The single place served-index bytes become a `ShardState`, whatever wrote them.
///
/// Sniffs the container magic, so JSON written by any earlier binary keeps loading unchanged and
/// a container written by a newer one is refused with a clear error rather than mis-parsed. Every
/// decode site goes through here; the previous attempt at a binary index failed precisely because
/// the decoders were scattered and could not move together.
pub(super) fn decode_index_bytes(bytes: &[u8]) -> Result<ShardState, String> {
    if !bytes.starts_with(INDEX_CONTAINER_MAGIC) {
        return serde_json::from_slice::<ShardState>(bytes).map_err(|error| error.to_string());
    }
    let body = &bytes[INDEX_CONTAINER_MAGIC.len()..];
    let (codec, payload) = body
        .split_first()
        .ok_or_else(|| "served index container is truncated".to_string())?;
    match *codec {
        INDEX_CODEC_ZSTD_JSON => {
            let json = zstd::stream::decode_all(payload)
                .map_err(|error| format!("served index payload did not decompress: {error}"))?;
            serde_json::from_slice::<ShardState>(&json).map_err(|error| error.to_string())
        }
        INDEX_CODEC_ZSTD_MSGPACK => {
            if payload.len() < INDEX_BINARY_VERSION_BYTES {
                return Err("served index binary payload is truncated".to_string());
            }
            let (version_bytes, body) = payload.split_at(INDEX_BINARY_VERSION_BYTES);
            let version = u32::from_be_bytes(
                version_bytes
                    .try_into()
                    .map_err(|_| "served index version stamp is malformed".to_string())?,
            );
            // Checked BEFORE decoding, deliberately. This payload is addressed by field order, so
            // decoding it against a different struct shape does not error, it produces a plausible
            // and wrong `ShardState`. Refusing is treated like an absent index: the caller replays
            // the WAL and the index-log deltas, which is slower and correct.
            if version != SHARD_INDEX_FORMAT_VERSION {
                return Err(format!(
                    "served index was written from struct version {version}, this binary is {}",
                    SHARD_INDEX_FORMAT_VERSION
                ));
            }
            let decoded = zstd::stream::decode_all(body)
                .map_err(|error| format!("served index payload did not decompress: {error}"))?;
            rmp_serde::from_slice::<ShardState>(&decoded).map_err(|error| error.to_string())
        }
        other => Err(format!(
            "served index uses payload codec {other}, which this binary cannot read"
        )),
    }
}

/// Collect the served-index delta items for exactly the object keys a single write
/// touched. Looks up only the routing buckets those keys map to (never the whole store),
/// so the result is O(delta): one `IndexItem` per live/tombstoned page currently backing a
/// touched key. Deleted pages ride as `deleted` tombstones so a fold applies the removal.
/// Empty when the command has no object keys (e.g. a context rebuild command); the caller
/// still appends the record so the index-log sequence advances per write.
/// The (kind, object_key, component) writes a command performs when -- and only when -- every
/// one of them lands through the page-upsert path (one new page per component, predecessor
/// replaced). `None` = the command's write shape is not a pure upsert (deletes, features,
/// rewrites), and the caller must fall back to the whole-object snapshot record.
/// Emission gate for upsert delta records. ON by default; TS_INDEXLOG_UPSERT_DELTAS=0 is the
/// escape hatch. The gate was OFF while a multi-restart scale store that reconstructed EMPTY
/// on reload was attributed to the fold of a large upsert-record log. The scale
/// reload-equality suite (tests/upsert_reload_equality.rs: thousands of batch-committed
/// upsert records, config-log present, threshold dumps, SIGKILL restarts across two
/// generations, verified under BOTH recovery modes) plus an engine-level reload of the
/// preserved damaged store under every mode/artifact combination showed the fold reconstructs
/// the full served view; the observed emptiness came from the serving layer answering
/// vacuously (a discarded shard-load failure served as an empty store) and is fixed there.
fn upsert_deltas_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("TS_INDEXLOG_UPSERT_DELTAS")
            .map(|value| !matches!(value.trim(), "0" | "false" | "no" | "off"))
            .unwrap_or(true)
    })
}

fn command_upsert_components(
    command: &Command,
) -> Option<Vec<(&'static str, String, Option<String>)>> {
    match command {
        Command::HashSet { key, field, .. } => {
            Some(vec![("hash", key.clone(), Some(field.clone()))])
        }
        Command::HashMultiSet { key, entries } => Some(
            entries
                .iter()
                .map(|(field, _)| ("hash", key.clone(), Some(field.clone())))
                .collect(),
        ),
        Command::HashIncrBy { key, field, .. } => {
            Some(vec![("hash", key.clone(), Some(field.clone()))])
        }
        Command::StringSet { key, .. } => Some(vec![("string", key.clone(), None)]),
        _ => None,
    }
}

/// Build the exact index items for an upsert record from post-apply shard state: each written
/// component's address is read back from the map the write just updated, so the logged page is
/// precisely the one a reload must serve. A component absent from the map (its append failed)
/// is skipped -- it produced no page to pin.
fn collect_upsert_index_items(
    shard: &ShardState,
    shard_id: ShardId,
    components: &[(&'static str, String, Option<String>)],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> Vec<crate::index_log::IndexItem> {
    let mut items = Vec::with_capacity(components.len());
    for (kind, object_key, component) in components {
        let address = match (*kind, component) {
            ("hash", Some(field)) => shard
                .hashes
                .get(object_key)
                .and_then(|fields| fields.get(field))
                .cloned(),
            ("string", None) => shard.strings.get(object_key).cloned(),
            _ => None,
        };
        let Some(address) = address else { continue };
        let routing_bucket = address
            .routing_bucket
            .unwrap_or_else(|| {
                page_routing_bucket(object_key, start_routing_bucket, end_routing_bucket)
            });
        let object_id = address.object_id.unwrap_or_else(|| {
            stable_page_object_id(shard_id, kind, object_key, component.as_deref())
        });
        let page_ref_key = format!(
            "{}:{}:{}:{}:{}:{}:{}:{}",
            kind,
            object_key,
            component.as_deref().unwrap_or(""),
            address.page_slab_id,
            address.offset,
            address.length,
            address.page_id.unwrap_or_default(),
            address.generation.unwrap_or_default()
        );
        items.push(crate::index_log::IndexItem {
            kind: crate::index_log::IndexItemKind::Page,
            routing_bucket,
            page_ref_key,
            object_key: object_key.clone(),
            model_id: (*kind).to_string(),
            component: component.clone(),
            object_id,
            page_id: address.page_id.unwrap_or(0),
            size: address.length,
            in_log: address.page_id.is_none(),
            deleted: false,
            address: Some(address),
        });
    }
    items
}

fn collect_command_index_items(
    shard: &ShardState,
    command_keys: &[String],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> Vec<crate::index_log::IndexItem> {
    use std::collections::BTreeSet;
    let keys: BTreeSet<&str> = command_keys.iter().map(String::as_str).collect();
    if keys.is_empty() {
        return Vec::new();
    }
    let buckets: BTreeSet<u32> = keys
        .iter()
        .map(|key| page_routing_bucket(key, start_routing_bucket, end_routing_bucket))
        .collect();
    let mut items = Vec::new();
    for routing_bucket in buckets {
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            continue;
        };
        for (page_ref_key, page) in &bucket.page_index {
            if !keys.contains(page.object_key.as_str()) {
                continue;
            }
            items.push(crate::index_log::IndexItem {
                kind: crate::index_log::IndexItemKind::Page,
                routing_bucket,
                page_ref_key: page_ref_key.clone(),
                object_key: page.object_key.clone(),
                model_id: page.model_id.clone(),
                component: page.component.clone(),
                object_id: page.object_id,
                page_id: page.address.page_id.unwrap_or(0),
                address: Some(page.address.clone()),
                size: page.address.length,
                in_log: page.log_backed,
                deleted: page.deleted,
            });
        }
    }
    items
}

/// Capture the authoritative post-write state of the maps that a single page-index entry
/// cannot reconstruct on reload, for exactly the object keys a write touched (O(delta)):
///  - packed timestamped series (features + the context timestamped maps): one physical
///    page holds many timestamps, so an eviction that trims the in-memory membership leaves
///    the dropped timestamps physically in the page. Pinning the membership here stops
///    reconstruction-from-pages resurrecting them.
///  - non-page maps that ride only on the serialized index (TTL expiry, control-state
///    change/sketch/selection, context nodes/entities/embeddings), which no page entry
///    encodes.
/// Each blob is `{"key": ..., "<map>": <value-or-null>}`; a null marks the key absent from
/// that map (a tombstone). Opaque JSON so the index-log layer stays decoupled from the
/// concrete `ShardState` field types.
fn capture_key_states(shard: &ShardState, keys: &[String]) -> Vec<serde_json::Value> {
    keys.iter()
        .map(|key| {
            serde_json::json!({
                "key": key,
                "features": shard.features.get(key),
                "expires_at_ms": shard.expires_at_ms.get(key),
                "control_state_changes": shard.control_state_changes.get(key),
                "control_state_change_sketch": shard.control_state_change_sketch.get(key),
                "control_state_selection": shard.control_state_selection.get(key),
                "context_nodes": shard.context_nodes.get(key),
                "context_events": shard.context_events.get(key),
                "context_indexes": shard.context_indexes.get(key),
                "context_audits": shard.context_audits.get(key),
                "context_children": shard.context_children.get(key),
                "context_summaries": shard.context_summaries.get(key),
                "context_compressions": shard.context_compressions.get(key),
                "context_entities": shard.context_entities.get(key),
            })
        })
        .collect()
}

/// Apply the captured per-key state blobs back onto a shard (last blob wins per key),
/// pinning the authoritative membership a write produced. Used on reload after WAL replay
/// to correct the exact touched keys, so reconstruction from physical pages honors the
/// evicted/tombstoned membership instead of resurrecting it.
fn apply_key_states(shard: &mut ShardState, key_states: &[serde_json::Value]) {
    for blob in key_states {
        let Some(key) = blob.get("key").and_then(|value| value.as_str()) else {
            continue;
        };
        apply_key_state_field(&mut shard.features, key, blob.get("features"));
        apply_key_state_field(&mut shard.expires_at_ms, key, blob.get("expires_at_ms"));
        apply_key_state_field(
            &mut shard.control_state_changes,
            key,
            blob.get("control_state_changes"),
        );
        apply_key_state_field(
            &mut shard.control_state_change_sketch,
            key,
            blob.get("control_state_change_sketch"),
        );
        apply_key_state_field(
            &mut shard.control_state_selection,
            key,
            blob.get("control_state_selection"),
        );
        apply_key_state_field(&mut shard.context_nodes, key, blob.get("context_nodes"));
        apply_key_state_field(&mut shard.context_events, key, blob.get("context_events"));
        apply_key_state_field(&mut shard.context_indexes, key, blob.get("context_indexes"));
        apply_key_state_field(&mut shard.context_audits, key, blob.get("context_audits"));
        apply_key_state_field(&mut shard.context_children, key, blob.get("context_children"));
        apply_key_state_field(&mut shard.context_summaries, key, blob.get("context_summaries"));
        apply_key_state_field(
            &mut shard.context_compressions,
            key,
            blob.get("context_compressions"),
        );
        apply_key_state_field(&mut shard.context_entities, key, blob.get("context_entities"));
    }
}

/// Set or clear one key's entry, in whichever map holds it.
///
/// Written against the operations it actually uses -- insert and remove -- so that a map can be
/// kept in key order where that matters without this having to care.
/// The two things [`apply_key_state_field`] does to a map, so it does not have to name the map.
trait KeyedState<V> {
    fn insert_entry(&mut self, key: String, value: V);
    fn remove_entry(&mut self, key: &str);
}

impl<V> KeyedState<V> for std::collections::HashMap<String, V> {
    fn insert_entry(&mut self, key: String, value: V) {
        self.insert(key, value);
    }
    fn remove_entry(&mut self, key: &str) {
        self.remove(key);
    }
}

impl<V> KeyedState<V> for std::collections::BTreeMap<String, V> {
    fn insert_entry(&mut self, key: String, value: V) {
        self.insert(key, value);
    }
    fn remove_entry(&mut self, key: &str) {
        self.remove(key);
    }
}

fn apply_key_state_field<V, M>(map: &mut M, key: &str, value: Option<&serde_json::Value>)
where
    V: serde::de::DeserializeOwned,
    M: KeyedState<V>,
{
    match value {
        Some(value) if !value.is_null() => {
            if let Ok(parsed) = serde_json::from_value::<V>(value.clone()) {
                map.insert_entry(key.to_string(), parsed);
            }
        }
        _ => {
            map.remove_entry(key);
        }
    }
}

/// Apply one delta record's page items to the bucket index, making the delta's view of the
/// covered object keys authoritative: every existing live page entry for a covered key is
/// removed first (so an overwrite that relocated or dropped a page does not leave the stale
/// entry behind), then the delta's live items are inserted at their ORIGINAL recorded
/// addresses. This is what lets reload reconstruct the exact on-disk page layout without
/// re-executing the WAL (which would write fresh pages and relocate them to the active
/// slab). `covered_keys` are the object keys the write touched (from the key-state blobs).
fn fold_delta_page_items(
    bucket_index: &mut CoreIndex,
    covered_keys: &BTreeSet<String>,
    items: &[crate::index_log::IndexItem],
    upsert: bool,
) {
    if upsert {
        // Upsert record: each item replaces exactly its (kind, object, component) predecessor,
        // the same replacement the write path performed in memory. The predecessor lives in the
        // same routing bucket (the bucket derives from the object key), so the removal scans one
        // bucket per item and the covered-key wipe below stays untouched for snapshot records.
        for item in items {
            let Some(bucket) = bucket_index.bucket_map.get_mut(&item.routing_bucket) else {
                continue;
            };
            bucket.page_index.retain(|_, page| {
                !(page.model_id == item.model_id
                    && page.object_key == item.object_key
                    && page.component == item.component)
            });
        }
    } else if !covered_keys.is_empty() {
        for bucket in bucket_index.bucket_map.values_mut() {
            bucket
                .page_index
                .retain(|_, page| !covered_keys.contains(&page.object_key));
        }
    }
    for item in items {
        if item.deleted {
            continue;
        }
        let Some(address) = item.address.clone() else {
            continue;
        };
        let bucket = bucket_index
            .bucket_map
            .entry(item.routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket: item.routing_bucket,
                meta_loaded: true,
                in_memory: true,
                ..BucketNode::default()
            });
        bucket.object_index.insert(item.object_id);
        bucket.page_index.insert(
            item.page_ref_key.clone(),
            PageIndex {
                object_key: item.object_key.clone(),
                model_id: item.model_id.clone(),
                component: item.component.clone(),
                object_id: item.object_id,
                address,
                dirty: false,
                deleted: false,
                log_backed: item.in_log,
            },
        );
    }
}

/// Collect the object keys a delta record touched, read from its per-key state blobs.
fn delta_record_covered_keys(record: &crate::index_log::IndexDeltaRecord) -> BTreeSet<String> {
    let mut keys: BTreeSet<String> = record
        .key_states
        .iter()
        .filter_map(|blob| blob.get("key").and_then(|value| value.as_str()))
        .map(str::to_string)
        .collect();
    // Fall back to the items' own object keys if a record carried page items but no blobs.
    for item in &record.items {
        keys.insert(item.object_key.clone());
    }
    keys
}

thread_local! {
    // Set while replaying the WAL into a shard on load. Writes issued during replay
    // must NOT re-append to the WAL (they are already logged) and must not re-persist
    // the index per record; the reconstructed index is persisted once when replay
    // finishes. Thread-local because replay runs synchronously on the loading thread.
    static REPLAYING_WAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn replaying_wal() -> bool {
    REPLAYING_WAL.with(|cell| cell.get())
}

thread_local! {
    // Set while applying a committed raft entry to the state machine. On this path the raft
    // log is the durability source (quorum-replicated + fsync'd), and a node reconstructs on
    // restart by loading the base and REPLAYING the raft log from the snapshot/base anchor --
    // which re-executes the commands and rebuilds the served index. The per-apply index-log
    // fsync is therefore redundant on the critical replication path (it only slows apply and
    // widens the snapshot-transfer window), so the index-log delta is appended NON-BLOCKING
    // (buffered, no fsync). Losing a non-fsync'd index-log tail on crash is safe: raft replay
    // rebuilds it. Thread-local because raft apply runs synchronously on the apply thread.
    static RAFT_APPLYING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn raft_applying() -> bool {
    RAFT_APPLYING.with(|cell| cell.get())
}

struct RaftApplyGuard;

impl RaftApplyGuard {
    fn enter() -> Self {
        RAFT_APPLYING.with(|cell| cell.set(true));
        RaftApplyGuard
    }
}

impl Drop for RaftApplyGuard {
    fn drop(&mut self) {
        RAFT_APPLYING.with(|cell| cell.set(false));
    }
}

thread_local! {
    // Set while applying a COMMITTED raft batch under TS_RAFT_APPLY_COALESCE. While set, each
    // per-command WAL append reserves its sequence with sync=false (append_for_group_commit) and
    // records the reserved sequence in RAFT_APPLY_BATCH_BARRIER instead of taking its own
    // fdatasync; `execute_raft_apply_batch` issues ONE coalesced `commit_barrier` for the whole
    // batch after every command is applied. Thread-local because raft apply runs synchronously on
    // the apply thread. A raft group is one shard, so the accumulator holds a single (shard, seq).
    static RAFT_APPLY_BATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static RAFT_APPLY_BATCH_BARRIER: std::cell::RefCell<Option<(ShardId, u64)>> =
        const { std::cell::RefCell::new(None) };
}

fn raft_apply_batch_active() -> bool {
    RAFT_APPLY_BATCH.with(|cell| cell.get())
}

fn record_raft_apply_batch_barrier(shard_id: ShardId, sequence: u64) {
    RAFT_APPLY_BATCH_BARRIER.with(|cell| {
        let mut slot = cell.borrow_mut();
        match *slot {
            Some((existing_shard, existing_seq)) if existing_shard == shard_id => {
                *slot = Some((existing_shard, existing_seq.max(sequence)));
            }
            _ => *slot = Some((shard_id, sequence)),
        }
    });
}

/// Drop-guarded batch scope: sets RAFT_APPLY_BATCH on enter (clearing any stale barrier) and clears
/// it on drop, so a panic mid-batch cannot leave the thread stuck in batch mode.
struct RaftApplyBatchGuard;

impl RaftApplyBatchGuard {
    fn enter() -> Self {
        RAFT_APPLY_BATCH.with(|cell| cell.set(true));
        RAFT_APPLY_BATCH_BARRIER.with(|cell| *cell.borrow_mut() = None);
        RaftApplyBatchGuard
    }

    fn take_barrier(&self) -> Option<(ShardId, u64)> {
        RAFT_APPLY_BATCH_BARRIER.with(|cell| cell.borrow_mut().take())
    }
}

impl Drop for RaftApplyBatchGuard {
    fn drop(&mut self) {
        RAFT_APPLY_BATCH.with(|cell| cell.set(false));
    }
}

thread_local! {
    // During a replayed command, the leader's wall-clock timestamp captured in the
    // replayed record's metadata. Time-dependent resolution (TTL deadlines, context
    // event time) reads this instead of the live clock so a re-executed command
    // resolves the SAME absolute value the leader did (resolve-then-log), keeping
    // replay deterministic across crash recovery and followers instead of drifting to a
    // later restart-time deadline.
    static REPLAY_CLOCK_MS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(super) fn set_replay_clock_ms(clock_ms: Option<u64>) {
    REPLAY_CLOCK_MS.with(|cell| cell.set(clock_ms));
}

/// Wall-clock time for deadline / event-time stamping. Returns the replay clock (the
/// leader timestamp of the record being replayed) when set, otherwise the live clock.
pub(super) fn resolve_now_ms() -> u64 {
    REPLAY_CLOCK_MS
        .with(|cell| cell.get())
        .unwrap_or_else(now_ms)
}

struct WalReplayGuard;

impl WalReplayGuard {
    fn enter() -> Self {
        REPLAYING_WAL.with(|cell| cell.set(true));
        WalReplayGuard
    }
}

impl Drop for WalReplayGuard {
    fn drop(&mut self) {
        REPLAYING_WAL.with(|cell| cell.set(false));
        REPLAY_CLOCK_MS.with(|cell| cell.set(None));
    }
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    atomic_write_bytes_synced(path, bytes, true)
}

/// Atomic temp-write + rename. When `sync` is true the temp file is `fsync`'d before the
/// rename (crash-durable). When false, the content + rename are still issued (so the new
/// bytes are immediately visible to any reader via the page cache), but the durability
/// barrier is DEFERRED. Deferral is safe ONLY for the served-index checkpoint on the
/// write/ack path: the WAL (durably synced before ack) is the recovery source of truth
/// and replay rebuilds the served index from it, so a stale-on-crash index just replays a
/// longer WAL suffix -- no acked write is lost. Durability-critical writers (dump
/// manifest, manifest install-on-load) MUST pass sync=true.
fn atomic_write_bytes_synced(path: &Path, bytes: &[u8], sync: bool) -> Result<(), std::io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        next_temp_counter()
    ));
    let write_result = (|| {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        if sync {
            file.sync_all()?;
        }
        drop(file);
        fs::rename(&temp_path, path)?;
        if sync {
            // The rename is only crash-durable once the PARENT DIRECTORY entry is fsync'd:
            // sync_all above makes the temp file's data+inode durable, but the rename that
            // publishes it under `path` is a directory mutation that can still be lost on a
            // crash. This backs the dump manifest (the durable WAL-reclaim watermark), the
            // base index, and the install markers -- if the rename is not durable, a dump can
            // let WAL-GC truncate the WAL to the manifest watermark, then a crash loses the
            // manifest directory entry while the reclaimed WAL is already gone = permanent
            // acked-write loss. Every other durable writer (wal.rs, index_log.rs,
            // block_store) already syncs the parent dir here.
            sync_parent_dir(path)?;
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

/// Make the atomic-rename that published `path` crash-durable by fsync'ing the parent
/// directory entry (mirrors `wal::sync_parent_dir` / `index_log::sync_parent_dir`).
fn sync_parent_dir(path: &Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            dir.sync_all()?;
        }
    }
    Ok(())
}

/// TS_WAL_LEGACY_RECOVERY: emergency escape hatch. When set, the engine falls back to the
/// legacy multi-barrier write path (WAL + data-page + served-index delta all fsync'd on the ack
/// path) AND delta-fold recovery. Default OFF -> the single write-path durability barrier
/// (WAL-only fsync) + base-only recovery is the DEFAULT. This exists solely so an operator can
/// revert the default in the field without a rebuild; steady state runs single-barrier.
pub(super) fn wal_legacy_recovery() -> bool {
    env_flag_on("TS_WAL_LEGACY_RECOVERY")
}

/// On the write/ack path only the WAL takes a synchronous durability barrier; the served-index
/// checkpoint write is issued but its fsync is deferred to the background flush / OS writeback
/// (reconstructable from the WAL on recovery). Implied by the single-barrier default; disabled
/// only by the TS_WAL_LEGACY_RECOVERY escape hatch.
pub(super) fn wal_only_sync() -> bool {
    wal_single_barrier()
}

/// TS_WAL_SINGLE_BARRIER: the true SINGLE write-path durability barrier. Only the WAL takes a
/// synchronous fdatasync per write (1.00/write); the data-page fdatasync, the served-index
/// delta-log fdatasync, the band-manifest persist, and the base-index sync are all deferred.
/// Correctness rests on the WAL + the durable dump checkpoint being a COMPLETE source of truth:
///  - config changes (feature_max_size etc.) become durable and WAL-sequence-ordered via a
///    per-shard config-log, so replay re-derives config-driven eviction (trims) at the exact
///    frontier they took effect (see `append_config_log_entry` / `config_log_entries`). Without
///    this, WAL-only replay re-executed feature appends with the default config and resurrected
///    evicted points.
///  - expiry (TTL) resolves against the leader timestamp captured in each WAL record (replay
///    clock) and applies lazily; compaction is background + non-destructive to logical
///    membership -- both already WAL-re-derivable.
///  - `flush_shard_index` fsyncs every data page (and the WAL) BEFORE advancing the dump
///    watermark, so every page at/below the watermark is durable. Recovery is BASE-ONLY: it
///    trusts only the durable base/manifest checkpoint (never the un-synced delta or the anchor
///    it advances), then replays the WAL tail from the watermark, re-deriving every post-dump
///    page EXACTLY ONCE. A page written but never fsync'd is rebuilt from its WAL command rather
///    than left dangling -- no page loss, no double-apply.
/// Default ON (the productionized write/recovery path). Set TS_WAL_LEGACY_RECOVERY=1 to fall
/// back to the legacy multi-barrier write path + delta-fold recovery.
pub(super) fn wal_single_barrier() -> bool {
    !wal_legacy_recovery()
}

/// TS_ENGINE_CONCURRENT_COMMIT: run the WAL durability barrier OUTSIDE the global `shards`
/// write lock. When ON, a synchronous write reserves its WAL sequence and appends its record
/// UNDER the `shards` lock (preserving WAL-order == apply-order), then RELEASES the lock and
/// awaits the durable barrier (`commit_barrier`). This lets concurrent same-shard writers reach
/// the group-commit queue while a peer's fdatasync is in flight, so #45's fsync coalescing
/// actually engages (fewer fsyncs than writes; QPS scales with concurrency). Default OFF ->
/// byte-identical to the legacy in-lock `append_with_sync` barrier. The ack is always returned
/// strictly AFTER the covering barrier succeeds, so durability is never weakened.
fn engine_concurrent_commit() -> bool {
    env_flag_default_on("TS_ENGINE_CONCURRENT_COMMIT")
}

/// TS_PHASE1_FLAT: make phase-1 (the work under the global `shards` write lock in
/// `execute_with_storage_override`) O(1) per write so it stops aging O(n) with data size. Two
/// per-write O(store) costs otherwise run under the lock on the live path: (1) the WAL append's
/// full-file `last_wal_sequence_at` rescan (handled in `wal.rs` by the same gate), and (2) the
/// per-execute `promote_model_maps_to_bucket_index_authority` reconciliation scan at the top of
/// `execute_with_storage_override`, which walks + clones every live model-map entry only to
/// re-confirm that `bucket_index` (which every write already keeps authoritative) is in sync. With
/// the gate on, once a promote scan has confirmed sync (`ShardState.promote_scan_done`) the hot
/// path skips the repeat scan. Default OFF -> byte-identical (the scan runs every command exactly
/// as before). Sharing one gate with the WAL fast-append so a single switch flattens phase-1.
fn phase1_flat_enabled() -> bool {
    env_flag_default_on("TS_PHASE1_FLAT")
}

/// TS_RAFT_APPLY_COALESCE: on the raft state-machine apply path, coalesce the per-committed-entry
/// engine-WAL fdatasync across a whole committed batch (one fsync per AppendEntries batch / recovery
/// replay / pipelined-propose group instead of one per entry) and anchor the served index off the
/// O(1) cached WAL sequence. Default OFF -> per-entry `execute_raft_apply` (byte-identical). The
/// raft log stays the durability + reconstruction source; the coalesced barrier still completes
/// before the raft runtime advances the durable applied_index.
fn raft_apply_coalesce() -> bool {
    env_flag_default_on("TS_RAFT_APPLY_COALESCE")
}

fn env_flag_on(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Default-ON gate read: the fix is LIVE unless explicitly disabled with
/// `=0|false|no|off`. Shipped write-path/raft fixes use this so production gets the
/// fixed behavior by default; the env var remains only as an escape hatch.
/// Bound eviction victim selection to a sampled scan instead of enumerating and sorting every
/// bucket. Off by default: this changes which buckets are chosen, not just how fast they are
/// found, so it wants deliberate enabling and measurement per deployment.
pub fn evict_sampled_lru_enabled() -> bool {
    env_flag_on("TS_EVICT_SAMPLED_LRU")
}

/// Tuning for sampled eviction, read from the environment with defaults that mirror the
/// established policy: sample several buckets per wanted victim, keep a bounded candidate pool
/// across passes, and cap how far one pass may walk.
pub(crate) fn evict_sampler_config() -> eviction_sampler::EvictionSamplerConfig {
    fn parse(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default)
    }
    eviction_sampler::EvictionSamplerConfig {
        samples: parse("TS_EVICT_SAMPLES", 5),
        pool_size: parse("TS_EVICT_POOL_SIZE", 64),
        scan_turns: parse("TS_EVICT_SCAN_TURNS", 4),
    }
}

fn env_flag_default_on(name: &str) -> bool {
    !matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

/// One durable config-log entry: the shard config `config`, effective for every WAL write with
/// sequence strictly greater than `after_seq`. Written under single-barrier mode so WAL-tail
/// replay re-derives config-driven eviction (feature_max_size trims) at the exact frontier the
/// change took effect, rather than replaying with a lost/default config and resurrecting or
/// dropping points.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct ConfigLogEntry {
    pub after_seq: u64,
    pub config: Config,
}

fn next_temp_counter() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
fn unique_temp_path(kind: &str) -> PathBuf {
    crate::scratch::unique_temp_path(kind)
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

/// Configurable, reference-style temporal-compression trigger for one context node.
///
/// Called on the event-write path (guarded by policy, disabled by default). When a
/// node crosses the configured raw-event count or age threshold, it folds the oldest
/// pending window of raw events (keeping the newest `keep_recent_events` raw) into a
/// single `ContextCompressionEvent`. Bounded to one window per call, so writes stay
/// light; the per-node high-water mark advances in-memory. Non-destructive: raw
/// events remain queryable/replayable (physical GC stays a separate concern).
/// Returns true if it wrote a compression record. Entities are never touched.
fn maybe_auto_compress_context_node(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    tenant_hash: u64,
    node_hash: u64,
    event_object_key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let policy = context_compression_policy_from_env();
    if !policy.enabled {
        return false;
    }
    let event_times: Vec<u64> = match shard.context_events.get(event_object_key) {
        Some(series) => series
            .keys()
            .map(|timeline_key| timeline_key / CONTEXT_TIMELINE_FANOUT)
            .collect(),
        None => return false,
    };
    let watermark = shard
        .context_compression_watermark
        .get(event_object_key)
        .copied()
        .unwrap_or(0);
    let window = match plan_next_compression_window(&policy, &event_times, watermark, now_ms()) {
        Some(window) => window,
        None => return false,
    };
    // Stable compression id per (node, window) makes re-processing idempotent: the
    // same window maps to the same timeline key and overwrites rather than duplicates.
    let compression_id_hash =
        stable_object_hash(&format!("compress:{node_hash}:{}:{}", window.start_ms, window.end_ms));
    let compression_event = ContextCompressionEvent {
        compression_id_hash,
        node_hash,
        source_start_ms: window.start_ms,
        source_end_ms: window.end_ms,
        compressed_time_ms: now_ms(),
        summary: format!(
            "Auto temporal compression: {} context events for node {} in window [{}, {}].",
            window.count, node_hash, window.start_ms, window.end_ms
        ),
    };
    let compression_key = context_compression_key(tenant_hash, node_hash);
    let timeline_key = context_timeline_key(window.start_ms, compression_id_hash);
    let routing_bucket = page_routing_bucket(&compression_key, start_routing_bucket, end_routing_bucket);
    let mut mutated = false;
    if let Ok(addresses) = append_timestamped_kv_pages(
        cache,
        page_store,
        shard_id,
        "context_compression",
        &compression_key,
        vec![FeaturePoint {
            timestamp_ms: timeline_key,
            value: context_bytes(&compression_event),
        }],
        routing_bucket,
        async_storage,
    ) {
        let series = shard
            .context_compressions
            .entry(compression_key.clone())
            .or_default();
        for (timestamp_ms, address) in addresses {
            series.insert(timestamp_ms, address);
            mutated = true;
        }
    }
    shard
        .context_compression_watermark
        .insert(event_object_key.to_string(), window.end_ms);
    invalidate_record_all(cache, shard_id, &compression_key);
    mutated
}

fn ttl_ms(shard: &mut ShardState, key: &str) -> i64 {
    if remove_if_expired(shard, key) {
        return -2;
    }
    if !record_exists(shard, key) {
        return -2;
    }
    associated_record_keys(key)
        .into_iter()
        .filter_map(|record_key| shard.expires_at_ms.get(&record_key).copied())
        .map(|expires_at| expires_at.saturating_sub(now_ms()) as i64)
        .min()
        .unwrap_or(-1)
}

/// The next `limit` deadlines after `cursor` that `keep` accepts, read straight out of the
/// ordered set.
///
/// The cost of a round should be the cost of its window. Asking `keep` about every deadline made it
/// the cost of the whole set instead -- about 11 ms per thousand deadlines for a window of sixteen,
/// on every cycle. Reading from where the cursor left off asks only about the window.
///
/// `scan_budget` bounds the walk: a long run of keys that `keep` rejects -- every resident key,
/// when the sweep is looking for the non-resident ones -- would otherwise still walk the set. The
/// cursor advances regardless, so the next round resumes past them and the sweep keeps moving.
pub(crate) fn expiry_window<F>(
    deadlines: &std::collections::BTreeMap<String, u64>,
    cursor: Option<&str>,
    limit: usize,
    scan_budget: usize,
    keep: F,
) -> (Vec<(String, u64)>, Option<String>)
where
    F: Fn(&str) -> bool,
{
    use std::ops::Bound::{Excluded, Unbounded};

    let lower = match cursor {
        Some(cursor) => Excluded(cursor.to_string()),
        None => Unbounded,
    };
    let mut selected = Vec::new();
    let mut walked = 0usize;
    let mut last_seen: Option<String> = None;
    let mut reached_the_end = true;
    for (key, expires_at) in deadlines.range((lower, Unbounded)) {
        if limit > 0 && selected.len() >= limit {
            reached_the_end = false;
            break;
        }
        if scan_budget > 0 && walked >= scan_budget {
            reached_the_end = false;
            break;
        }
        walked = walked.saturating_add(1);
        last_seen = Some(key.clone());
        if keep(key.as_str()) {
            selected.push((key.clone(), *expires_at));
        }
    }
    // Resume past everything examined, not past everything taken: the keys `keep` rejected were
    // looked at, and looking at them again next round is how a sweep fails to make progress.
    let next_cursor = if reached_the_end { None } else { last_seen };
    (selected, next_cursor)
}

fn remove_if_expired(shard: &mut ShardState, key: &str) -> bool {
    // Use the replay-aware clock: during WAL replay this resolves to the per-record leader
    // timestamp so lazy expiry reproduces the leader's original branch. Using the real
    // restart clock here would let a key that was live at leader-time (and thus took the
    // "exists" branch of a logged conditional write) appear expired on recovery, silently
    // dropping a durably-committed write and diverging the recovered state from the leader.
    let now = resolve_now_ms();
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        if shard
            .expires_at_ms
            .get(&record_key)
            .map(|expires_at| *expires_at <= now)
            .unwrap_or(false)
        {
            removed |= delete_record_exact(shard, &record_key);
        }
    }
    removed
}

fn delete_record(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        removed |= delete_record_exact(shard, &record_key);
    }
    removed
}

fn delete_record_exact(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    removed |= mark_bucket_index_object_deleted(shard, key);
    removed |= shard.expires_at_ms.remove(key).is_some();
    removed |= shard.strings.remove(key).is_some();
    removed |= shard.hashes.remove(key).is_some();
    removed |= shard.sets.remove(key).is_some();
    removed |= shard.lists.remove(key).is_some();
    removed |= shard.zsets.remove(key).is_some();
    removed |= shard.buckets.remove(key).is_some();
    removed |= shard.seen.remove(key).is_some();
    if shard.features.remove(key).is_some() {
        removed = true;
        control_rollup::feature_forget(shard, key);
    }
    if shard.control_state.remove(key).is_some() {
        removed = true;
        control_rollup::forget(shard, key);
    }
    removed |= shard.control_state_pages.remove(key).is_some();
    removed |= shard.control_state_changes.remove(key).is_some();
    removed |= shard.control_state_change_sketch.remove(key).is_some();
    removed |= shard.control_state_selection.remove(key).is_some();
    removed |= shard.context_nodes.remove(key).is_some();
    removed |= shard.context_events.remove(key).is_some();
    removed |= shard.context_indexes.remove(key).is_some();
    removed |= shard.context_audits.remove(key).is_some();
    removed |= shard.context_entities.remove(key).is_some();
    removed |= shard.context_children.remove(key).is_some();
    removed |= shard.context_summaries.remove(key).is_some();
    removed |= shard.context_compressions.remove(key).is_some();
    removed
}

fn mark_bucket_index_object_deleted(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    let target_buckets = bucket_index_target_buckets_for_object_key(shard, key);
    for routing_bucket in target_buckets {
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        let mut deleted_object_ids = BTreeSet::new();
        bucket.page_index.retain(|_, page| {
            if page.object_key == key {
                deleted_object_ids.insert(page.object_id);
                removed = true;
                false
            } else {
                true
            }
        });
        if !deleted_object_ids.is_empty() {
            bucket.object_index.extend(deleted_object_ids.iter().copied());
            bucket.deleted_object_index.extend(deleted_object_ids);
            bucket.dirty = true;
            bucket.deleted = bucket.page_index.is_empty();
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
            bucket.meta_loaded = true;
            bucket.in_memory = !bucket.page_index.is_empty();
            update_bucket_layout(bucket);
        }
    }
    if removed {
        shard.bucket_index.rebuild_object_page_lookup();
    }
    removed
}

fn bucket_index_target_buckets_for_object_key(shard: &ShardState, key: &str) -> BTreeSet<u32> {
    if shard.bucket_index.object_component_lookup.is_empty() {
        return shard.bucket_index.bucket_map.keys().copied().collect();
    }
    let mut buckets = BTreeSet::new();
    for kind in storage_model_kinds() {
        if let Some(page_refs) = shard
            .bucket_index
            .object_component_lookup
            .get(&object_component_lookup_key(kind, key))
        {
            buckets.extend(page_refs.iter().map(|page_ref| page_ref.routing_bucket));
        }
    }
    buckets
}

fn mark_bucket_index_page_deleted(
    shard: &mut ShardState,
    shard_id: ShardId,
    model_id: &str,
    key: &str,
    component: Option<&str>,
) -> bool {
    // Removing a member IS an outcome, and it is the one a command log states worst: replay has
    // to re-run the removal and hope the state it removes from matches. Saying "this component
    // is gone" needs no such hope. Recorded here because every typed removal comes through.
    if crate::wal::wal_outcome_items_enabled() {
        block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
            kind: model_id.to_string(),
            object_key: key.to_string(),
            component: component.map(str::to_string),
            object_id: stable_page_object_id(shard_id, model_id, key, component),
            routing_bucket: page_routing_bucket(key, 0, u32::MAX),
            address: None,
            value: None,
            ttl: None,
            deleted: true,
            meta: true,
        });
    }
    let mut removed = false;
    let target_buckets = if shard.bucket_index.object_page_lookup.is_empty() {
        shard
            .bucket_index
            .bucket_map
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
    } else {
        shard
            .bucket_index
            .object_page_lookup
            .get(&object_page_lookup_key(model_id, key, component))
            .map(|page_refs| {
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.routing_bucket)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default()
    };
    for routing_bucket in target_buckets {
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        let mut bucket_removed = false;
        let mut deleted_object_ids = BTreeSet::new();
        bucket.page_index.retain(|_, page| {
            let matches = page.model_id == model_id
                && page.object_key == key
                && page.component.as_deref() == component;
            if matches {
                deleted_object_ids.insert(page.object_id);
                bucket_removed = true;
                removed = true;
                false
            } else {
                true
            }
        });
        if bucket_removed {
            bucket.object_index.extend(deleted_object_ids.iter().copied());
            bucket.deleted_object_index.extend(deleted_object_ids);
            bucket.dirty = true;
            bucket.deleted = bucket.page_index.is_empty();
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
            bucket.meta_loaded = true;
            bucket.in_memory = !bucket.page_index.is_empty();
            update_bucket_layout(bucket);
        }
    }
    if removed {
        // Removes exactly the entry it deleted, instead of rebuilding the whole lookup.
        //
        // `rebuild_object_page_lookup` clears `object_page_lookup` and `object_component_lookup`
        // and re-inserts one entry per page in the shard -- 58 696 of them on a 250 MB store --
        // and this ran once per deleted page. A purge deleting five fields paid it five times.
        // That is why deleting an identical, freshly created memory cost 41.7 ms against a 20 MB
        // store and 385.7 ms against a 249 MB one, for provably the same closure: same four ids,
        // same 96 records scanned, same five fields rewritten. Identical work, nine times the
        // time, all of it spent rebuilding a lookup to the same shape it already had minus one
        // entry.
        //
        // This is the exact inverse of the `insert_object_page_lookup` the page went in through,
        // keyed on the same (model_id, object_key, component) -- which is how the upsert path
        // has always maintained the lookup. The whole-object deleter above still rebuilds; it
        // drops every component of a key at once, so the entry-at-a-time inverse does not apply
        // to it unchanged.
        shard
            .bucket_index
            .remove_object_page_lookup_entry(model_id, key, component);
    }
    removed
}

fn associated_record_keys(key: &str) -> Vec<String> {
    if key.starts_with("control_state:") {
        return vec![key.to_string()];
    }
    let mut keys = Vec::with_capacity(4);
    keys.push(key.to_string());
    for family in [ControlStateFamily::Counter, ControlStateFamily::Distinct, ControlStateFamily::Selection] {
        keys.push(control_state_family_key(family, key));
    }
    keys
}

fn collect_live_page_slab_ids(shard: &ShardState) -> BTreeSet<u64> {
    let mut ids = BTreeSet::new();
    ids.extend(
        shard
            .strings
            .values()
            .map(|address| address.page_slab_id),
    );
    for fields in shard.hashes.values() {
        ids.extend(fields.values().map(|address| address.page_slab_id));
    }
    for members in shard.sets.values() {
        ids.extend(members.values().map(|address| address.page_slab_id));
    }
    for elements in shard.lists.values() {
        ids.extend(elements.values().map(|address| address.page_slab_id));
    }
    for members in shard.zsets.values() {
        ids.extend(members.values().map(|(_, address)| address.page_slab_id));
    }
    for series in shard.features.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    ids.extend(
        shard
            .context_nodes
            .values()
            .map(|address| address.page_slab_id),
    );
    for series in shard.context_events.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    for series in shard.context_indexes.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    for series in shard.context_audits.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    for series in shard.context_entities.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    for series in shard.context_children.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    for series in shard.context_summaries.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    for series in shard.context_compressions.values() {
        ids.extend(series.values().map(|address| address.page_slab_id));
    }
    // control_state_pages is the page-backed control-state model and MUST be in the
    // GC live set: it feeds both the reclaim live-slab set and the page-gc dependency plan.
    // Omitting it let a slab holding only a control-state page be reclaimed while the index
    // still referenced it -> DataLoss on the next read. keeps any model's live pages
    // counted in the zone's used_bytes so the zone is never destroyed while referenced. The
    // sibling collect_model_live_page_entries already includes it -- the two lists had drifted.
    ids.extend(
        shard
            .control_state_pages
            .values()
            .map(|address| address.page_slab_id),
    );
    ids
}

fn append_value(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    bytes: &[u8],
    object_id: Option<u64>,
    routing_bucket: Option<u32>,
    async_storage: bool,
) -> Result<BlockAddress, BlockStoreError> {
    if !async_storage {
        return page_store.append_with_page_metadata(bytes, object_id, routing_bucket);
    }
    let address = BlockAddress {
        page_slab_id: HOT_PAGE_SLAB_ID,
        offset: HOT_PAGE_OFFSET.fetch_add(1, Ordering::Relaxed),
        length: bytes.len() as u64,
        page_id: None,
        object_id,
        routing_bucket,
        generation: object_id,
        band_id: None,
        sha256: None,
    };
    // Put the page aside for this write's record. It is often derived state rather than the
    // command's own bytes, so the record has to carry it for a read to serve it back.
    if block_in_wal::enabled() {
        if let Some(object_id) = object_id {
            block_in_wal::stage(object_id, bytes);
        }
    }
    let bytes = bytes.to_vec();
    cache.put_memory_only(
        CacheKey::page_with_slot(
            shard_id,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket),
        bytes,
    );
    Ok(address)
}

fn persist_control_state_page(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let Some(series) = shard.control_state.get(key) else {
        shard.control_state_pages.remove(key);
        return false;
    };
    // Coalesced-persistence mode: the counter series is durable via the index snapshot
    // (flush) + WAL replay — the same model control_state_changes/fol already use — so skip
    // the O(series) per-write whole-series page rewrite (the write-amplification source).
    // Gated on async_storage so the WAL actually covers between-flush increments.
    if async_storage && shard.control_coalesce_persist {
        return true;
    }
    let Ok(bytes) = serde_json::to_vec(series) else {
        return false;
    };
    let object_id = stable_page_object_id(shard_id, "control_state", key, None);
    let routing_bucket = page_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    if let Ok(address) = append_value(
        cache,
        page_store,
        shard_id,
        &bytes,
        Some(object_id),
        Some(routing_bucket),
        async_storage,
    ) {
        upsert_bucket_index_page(shard, shard_id, "control_state", key, None, address.clone(), true);
        shard.control_state_pages.insert(key.to_string(), address);
        true
    } else {
        false
    }
}

fn invalidate_cache_key(cache: &MultiLayerCache, key: CacheKey, memory_only: bool) {
    if memory_only {
        cache.invalidate_memory_only(&key);
    } else {
        let _ = cache.invalidate(&key);
    }
}

fn record_exists(shard: &ShardState, key: &str) -> bool {
    associated_record_keys(key)
        .iter()
        .any(|record_key| record_exists_exact(shard, record_key))
}

fn record_exists_exact(shard: &ShardState, key: &str) -> bool {
    let bucket_index_exists = if shard.bucket_index.object_component_lookup.is_empty() {
        shard.bucket_index.bucket_map.values().any(|bucket| {
            bucket.page_index
                .values()
                .any(|page| page.object_key == key && !page.deleted)
        })
    } else {
        storage_model_kinds().iter().any(|kind| {
            shard
                .bucket_index
                .object_component_lookup
                .get(&object_component_lookup_key(kind, key))
                .map(|page_refs| {
                    page_refs.iter().any(|page_ref| {
                        shard
                            .bucket_index
                            .bucket_map
                            .get(&page_ref.routing_bucket)
                            .and_then(|bucket| bucket.page_index.get(&page_ref.page_ref_key))
                            .map(|page| {
                                !page.deleted && page.model_id == *kind && page.object_key == key
                            })
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
        })
    };
    bucket_index_exists
        || shard.strings.contains_key(key)
        || shard.hashes.contains_key(key)
        || shard.sets.contains_key(key)
        || shard.lists.contains_key(key)
        || shard.zsets.contains_key(key)
        || shard.buckets.contains_key(key)
        || shard.seen.contains_key(key)
        || shard.features.contains_key(key)
        || shard.control_state.contains_key(key)
        || shard.control_state_pages.contains_key(key)
        || shard.control_state_changes.contains_key(key)
        || shard.control_state_selection.contains_key(key)
        || shard.context_nodes.contains_key(key)
        || shard.context_events.contains_key(key)
        || shard.context_indexes.contains_key(key)
        || shard.context_audits.contains_key(key)
        || shard.context_entities.contains_key(key)
        || shard.context_children.contains_key(key)
        || shard.context_summaries.contains_key(key)
        || shard.context_compressions.contains_key(key)
}

fn storage_model_kinds() -> &'static [&'static str] {
    &[
        "string",
        "hash",
        "set",
        "list",
        "zset",
        "feature",
        "sequence",
        "control_state",
        "context_node",
        "context_event",
        "context_index",
        "context_audit",
        "context_entity",
        "context_child",
        "context_embedding",
        "context_summary",
        "context_compression",
    ]
}

fn invalidate_record_all(cache: &MultiLayerCache, shard_id: ShardId, key: &str) {
    let _ = cache.invalidate(&CacheKey::string(shard_id, key));
    let _ = cache.invalidate_record(shard_id, "hash", key);
    let _ = cache.invalidate_record(shard_id, "set", key);
    let _ = cache.invalidate_record(shard_id, "feature", key);
}

fn read_page_bytes(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    address: &BlockAddress,
) -> Option<Vec<u8>> {
    let cache_key = CacheKey::page_with_slot(
        shard_id,
        address.page_slab_id,
        address.offset,
        address.length,
        address.routing_bucket);
    if let Ok(Some(bytes)) = cache.get(&cache_key) {
        return Some(bytes);
    }
    // Log-backed hot page (synthetic address, no block-store file): a cache miss here would read
    // as MISSING for an acked async write. If it was spilled to a real slab on eviction, resolve
    // the redirect and read the durable copy. On a genuine miss (never spilled, or spill failed)
    // this falls through to the normal read below, which returns None -- the WAL still holds the
    // value and a reload replays it.
    if crate::wal_record::is_wal_resident(address.page_slab_id) {
        if let Some(real_address) = hot_page_spill::lookup_spilled(shard_id, address.offset) {
            if let Ok(bytes) = page_store.read(&real_address) {
                let _ = cache.put(cache_key, bytes.clone());
                return Some(bytes);
            }
        }
        // Nothing spilled, so the value exists only in its WAL record -- which is where it has
        // been all along. Read it back by the log id the write registered. Tried after the
        // spill redirect because a spilled copy is a direct block-store read, while this one
        // parses a log record.
        if block_in_wal::enabled() {
            if let Some(bytes) =
                address
                    .object_id
                    .and_then(|object_id| block_in_wal::read_page(page_store, shard_id, object_id))
            {
                let _ = cache.put(cache_key, bytes.clone());
                return Some(bytes);
            }
        }
    }
    let bytes = page_store.read(address).ok()?;
    let _ = cache.put(cache_key, bytes.clone());
    Some(bytes)
}

fn read_page_bytes_cold(page_store: &LocalBlockStore, address: &BlockAddress) -> Option<Vec<u8>> {
    page_store.read(address).ok()
}

fn dedupe_nonzero_u64_preserve_order(values: Vec<u64>) -> Vec<u64> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| *value != 0 && seen.insert(*value))
        .collect()
}

fn cache_entry_routing_bucket(entry: &CacheEntryInfo) -> Option<u32> {
    entry
        .selector
        .strip_prefix("slot-")?
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn parse_i64(bytes: &Vec<u8>) -> Option<i64> {
    // Parse the integer with strtoll semantics (leading-whitespace tolerant):
    // strtoll skips leading whitespace, so a stored counter like " 5" is the valid integer 5.
    // Rust's str::parse rejects leading whitespace; trim it (ASCII only, so we do not accept
    // Unicode whitespace strtoll's isspace would reject). Trailing/embedded garbage still fails on
    // both sides (checks *end != '\0'), so only the previously-erroring leading-space case
    // changes.
    std::str::from_utf8(bytes)
        .ok()?
        .trim_start_matches(|c: char| c.is_ascii_whitespace())
        .parse()
        .ok()
}

fn object_manager_stats(
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> ObjectManagerStats {
    if !shard.bucket_index.bucket_map.is_empty() {
        let (bucket_object_count, bucket_page_ref_count, bucket_dirty_object_count) =
            if !shard.bucket_index.object_component_lookup.is_empty() {
                (
                    shard.bucket_index.object_component_lookup.len(),
                    shard
                        .bucket_index
                        .object_component_lookup
                        .values()
                        .map(BTreeSet::len)
                        .sum::<usize>(),
                    shard.dirty_objects.len(),
                )
            } else {
                let live_pages = shard
                    .bucket_index
                    .bucket_map
                    .values()
                    .flat_map(|bucket| bucket.page_index.values())
                    .filter(|page| !page.deleted)
                    .collect::<Vec<_>>();
                let bucket_object_count = live_pages
                    .iter()
                    .map(|page| {
                        (
                            page.model_id.as_str(),
                            page.object_key.as_str(),
                            (page.model_id == "hash")
                                .then(|| page.component.as_deref())
                                .flatten(),
                        )
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                let bucket_dirty_object_count = live_pages
                    .iter()
                    .filter(|page| page.dirty || shard.dirty_objects.contains(&page.object_key))
                    .map(|page| {
                        (
                            page.model_id.as_str(),
                            page.object_key.as_str(),
                            (page.model_id == "hash")
                                .then(|| page.component.as_deref())
                                .flatten(),
                        )
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                (bucket_object_count, live_pages.len(), bucket_dirty_object_count)
            };
        let secondary_object_count = shard.strings.len()
            + shard.hashes.len()
            + shard.sets.len()
            + shard.lists.len()
            + shard.zsets.len()
            + shard.features.len()
            + shard.control_state.len()
            + shard.control_state_changes.len()
            + shard.context_nodes.len()
            + shard.context_events.len()
            + shard.context_indexes.len()
            + shard.context_audits.len()
            + shard.context_entities.values().map(BTreeMap::len).sum::<usize>()
            + shard.context_children.len()
            + shard.context_summaries.len()
            + shard.context_compressions.len();
        let object_count = bucket_object_count.max(secondary_object_count);
        let dirty_object_count = bucket_dirty_object_count.max(shard.dirty_objects.len());
        let secondary_page_ref_count = shard.strings.len()
            + shard.hashes.values().map(HashMap::len).sum::<usize>()
            + shard.sets.values().map(BTreeMap::len).sum::<usize>()
            + shard.lists.values().map(BTreeMap::len).sum::<usize>()
            + shard.zsets.values().map(BTreeMap::len).sum::<usize>()
            + shard.features.values().map(BTreeMap::len).sum::<usize>()
            + shard.context_nodes.len()
            + shard
                .context_events
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard
                .context_indexes
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard
                .context_audits
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard.context_entities.values().map(BTreeMap::len).sum::<usize>()
            + shard
                .context_children
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard
                .context_summaries
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard
                .context_compressions
                .values()
                .map(BTreeMap::len)
                .sum::<usize>();
        let dirty_bucket_count = if !shard.bucket_index.object_component_lookup.is_empty() {
            let mut dirty_buckets = shard
                .bucket_index
                .bucket_map
                .iter()
                .filter_map(|(bucket_id, bucket)| bucket.dirty.then_some(*bucket_id))
                .collect::<BTreeSet<_>>();
            for object_key in &shard.dirty_objects {
                dirty_buckets.extend(bucket_index_target_buckets_for_object_key(shard, object_key));
            }
            dirty_buckets.len()
        } else {
            shard
                .bucket_index
                .bucket_map
                .values()
                .filter(|bucket| {
                    bucket.dirty
                        || bucket.page_index.values().any(|page| {
                            page.dirty || shard.dirty_objects.contains(&page.object_key)
                        })
                })
                .count()
        };
        return ObjectManagerStats {
            object_count,
            page_ref_count: bucket_page_ref_count.max(secondary_page_ref_count),
            dirty_object_count,
            dirty_bucket_count,
            routing_bucket_count: routing_bucket_count(start_routing_bucket, end_routing_bucket),
        };
    }

    let object_count = shard.strings.len()
        + shard.hashes.len()
        + shard.sets.len()
        + shard.lists.len()
        + shard.zsets.len()
        + shard.features.len()
        + shard.control_state.len()
        + shard.context_nodes.len()
        + shard.context_events.len()
        + shard.context_indexes.len()
        + shard.context_audits.len()
        + shard.context_entities.values().map(BTreeMap::len).sum::<usize>()
        + shard.context_children.len()
        + shard.context_summaries.len()
        + shard.context_compressions.len();
    let page_ref_count = shard.strings.len()
        + shard.hashes.values().map(HashMap::len).sum::<usize>()
        + shard.sets.values().map(BTreeMap::len).sum::<usize>()
        + shard.lists.values().map(BTreeMap::len).sum::<usize>()
        + shard.zsets.values().map(BTreeMap::len).sum::<usize>()
        + shard.features.values().map(BTreeMap::len).sum::<usize>()
        + shard.context_nodes.len()
        + shard
            .context_events
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_indexes
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_audits
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard.context_entities.values().map(BTreeMap::len).sum::<usize>()
        + shard
            .context_children
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_summaries
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_compressions
            .values()
            .map(BTreeMap::len)
            .sum::<usize>();
    let routing_bucket_count = routing_bucket_count(start_routing_bucket, end_routing_bucket);
    let mut dirty_buckets = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter_map(|(bucket, node)| node.dirty.then_some(*bucket))
        .collect::<BTreeSet<_>>();
    dirty_buckets.extend(
        shard
            .dirty_objects
            .iter()
            .map(|key| bucket_for_object(key, start_routing_bucket, routing_bucket_count)),
    );
    ObjectManagerStats {
        object_count,
        page_ref_count,
        dirty_object_count: shard.dirty_objects.len(),
        dirty_bucket_count: dirty_buckets.len(),
        routing_bucket_count,
    }
}

#[cfg(test)]
mod tests;
