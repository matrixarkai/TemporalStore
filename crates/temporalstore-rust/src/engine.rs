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
pub use storage_lifecycle_methods::{reset_storage_plan_build_counts, storage_plan_build_counts};
mod storage_manager_cycle;
mod storage_reports;
mod prometheus_metrics;
mod stream_batch_methods;
mod recovery_sweep_compact;
mod persistence;
mod bucket_dump_io;
pub use self::bucket_dump_io::{
    bucket_dump_manifest_io_counts, bucket_dump_manifest_listing_sites,
    bucket_dump_manifest_memo_len,
    reset_bucket_dump_manifest_io_counts, BucketDumpManifestIoCounts,
};
mod command_validation;
pub mod resource_blobs;
pub mod quota;
pub(crate) mod eviction_sampler;
// Single source of truth for write-command classification (shared with the data_node layer,
// which previously kept a drifted subset that mis-classified context/control-state writes
// as reads -> lifecycle-write-barrier bypass + missing dump scheduling).
pub(crate) use command_validation::{command_object_keys, is_write_command};
#[cfg(test)]
pub(crate) use storage_bucket_internals::uncovered_maintenance;
mod storage_bucket_internals;
pub use storage_bucket_internals::{
    bucket_block_index_visits, bucket_index_resident_bytes_visits, bucket_scoped_model_entries,
    bucket_visit_sites, layout_by_caller, live_block_scan_entries, live_block_scan_sites_snapshot,
    model_map_addresses_visited, reset_bucket_block_index_visits,
    reset_bucket_index_resident_bytes_visits, reset_bucket_scoped_model_entries,
    reset_live_block_scan_entries, reset_live_block_scan_sites, reset_model_map_addresses_visited,
    BLOCK_SLAB_LIVE_DRIFTS, BLOCK_SLAB_LIVE_RECONCILES,
};
pub use state::{block_slab_live_charges, reset_block_slab_live_charges};
pub use shard_write_guard::{
    index_encode_counts, maintenance_mirror_sink_lookups, maintenance_block_read_counts,
    reset_index_encode_counts, reset_maintenance_mirror_sink_lookups,
    reset_maintenance_block_read_counts, IndexEncodeCounts, MaintenanceBlockReadCounts,
};
mod compaction;
// The maintenance round in `data_node` asks this before compacting; see the function's doc.
pub use compaction::{compaction_drain_block_slab_ids, compaction_relocatable_block_refs};
mod storage_reporting;
pub(crate) mod hashing;
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
pub(crate) use self::constants::HOT_BLOCK_SLAB_ID;
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
use self::bucket_store::{read_bucket_index_value, bucket_index_component_block_addresses};
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
use crate::block_store::{BlockStore, BlockAddress, BlockStoreError, BlockStoreGcPolicy, BlockStoreOptions, BlockStoreSlabLive};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ContextCompressionEvent,
    ContextEntity, ContextEvent, ContextIndexRef, ContextNode, ContextPackAudit,
    context_node_summary_vector_enabled, context_vector_as_stored, ContextSummaryVector,
    CONTEXT_SUMMARY_LEVEL_L1,
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
    block_store: BlockStore,
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
    /// Whether a synchronous write takes its durable WAL barrier OUTSIDE the `shards` write lock.
    ///
    /// True everywhere but a test measuring what the other side of the lock costs.
    /// `TS_ENGINE_CONCURRENT_COMMIT` used to decide it for every engine in the process at once,
    /// which is why the tests that measure both sides had to be serialised against each other: a
    /// baseline could otherwise observe a window its sibling had opened. Shared across clones,
    /// because a clone is the same engine.
    /// The slab an unfinished compaction round is still filling, per shard, with the slab it
    /// rolled away from.
    ///
    /// A round rolls a fresh slab and relocates live pages onto it. Once a round is BOUNDED it
    /// can stop before every page has moved -- and rolling again next round would re-move
    /// everything the last one moved, so the next round continues filling this slab instead.
    /// That is what makes a bounded round make progress rather than shuffle the same pages.
    /// Absent means no round is in flight, so the next one rolls.
    ///
    /// Not durable: a restart loses at most the knowledge that a round was open, and the next
    /// round then rolls, which is correct if wasteful once.
    compaction_rounds: Arc<RwLock<HashMap<ShardId, (u64, u64)>>>,
    /// Where the bounded readability probe should START reading on this shard's next round.
    ///
    /// The probe reads at most `RECOVERY_READABLE_PROBE_PER_ROUND` live pages per round so its
    /// cost does not grow with the store. Without a resume position it began at the FIRST live
    /// page every round, so it re-read the same prefix forever and a page past that prefix was
    /// never read at all -- on a shard holding more live pages than the budget, the periodic
    /// loop could not discover an unreadable page outside the first window in any number of
    /// rounds, while the report it returned said the pages it HAD read were fine.
    ///
    /// Holding the index the next round resumes from turns that fixed prefix into a window that
    /// sweeps the whole shard and wraps, which is what makes a bounded round make PROGRESS
    /// rather than repeat itself -- the same reason `compaction_rounds` above is carried.
    ///
    /// An INDEX into the live-page vector, not a page identity: the vector is rebuilt each round
    /// and entries move, so a resumed round is not promised the exact page the previous one
    /// stopped before. That is acceptable for a sampler whose job is to cover the store over
    /// time; nothing durable depends on this position.
    ///
    /// Not durable, for the same reason `compaction_rounds` is not: a restart costs one repeated
    /// window, not correctness. Only BOUNDED callers advance it -- an unbounded call
    /// (`readable_probe_limit == 0`) reads every page anyway and leaves the position alone, so a
    /// diagnostic call cannot move the periodic loop's window out from under it.
    recovery_probe_cursors: Arc<RwLock<HashMap<ShardId, usize>>>,
    concurrent_commit: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the expiry sweep encodes and writes its served-index checkpoint while still
    /// holding the shard-table write guard.
    ///
    /// False everywhere but the control arm of the guard that measures what the in-lock flush
    /// costs. A guard that can only observe zero cannot distinguish a shortened hold from a
    /// counter that stopped counting; this is how an engine takes the other side.
    expiry_index_flush_under_lock: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the cache warm-up reads its pages while still holding the shard-table read guard.
    ///
    /// False everywhere but the control arm of the guard that measures it. Same reason as the
    /// flag above: an assertion that no page was read under a guard is satisfied just as well by
    /// a stage that read nothing, so the guard runs an arm that still reads them all under the
    /// lock and checks that the counter can reach the other answer.
    warm_cache_under_shard_guard: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the expiry sweep's served-index checkpoint is the WHOLE index rather than a
    /// delta of the keys the round removed.
    ///
    /// False everywhere but the arm that measures what the whole-index checkpoint cost. The
    /// whole-index write is kept reachable for exactly one reason: the claim "a round now
    /// persists what changed" is only falsifiable against an engine that still persists
    /// everything, measured in the same process on the same fixture. Without it a guard
    /// asserting flat bytes would pass just as well against a round that stopped writing.
    expiry_index_flush_whole: Arc<std::sync::atomic::AtomicBool>,
    /// Whether loading a shard warms the in-memory cache tier from the page store as part of
    /// the load, rather than leaving it to be warmed in the background.
    ///
    /// Starts from `MATRIXARK_EAGER_CACHE_WARM_ON_LOAD`, which the shipped config and several
    /// binaries set, so this is a deployment knob and not a retired switch. The proxy turns it
    /// off for its own engine, and used to do that by writing the variable into the process.
    eager_cache_warm: Arc<std::sync::atomic::AtomicBool>,
    /// Whether eviction picks its victims by a sampled scan instead of enumerating and sorting
    /// every bucket.
    ///
    /// FALSE. Sampling changes WHICH buckets are chosen, not only how fast they are found, so it
    /// wants deliberate enabling and measurement per deployment. One test turns it on, of the
    /// engine it built, to measure how the scan volume grows with the store either way.
    evict_sampled_lru: Arc<std::sync::atomic::AtomicBool>,
    /// Whether a committed raft batch shares ONE durable engine-WAL barrier.
    ///
    /// True everywhere but a test measuring the per-entry loop. `TS_RAFT_APPLY_COALESCE` used to
    /// decide it for every engine at once. Shared across clones, because a clone is the same
    /// engine.
    raft_apply_coalesce: Arc<std::sync::atomic::AtomicBool>,
    /// Diagnostics: number of per-execute `promote_model_maps_to_bucket_index_authority` full
    /// O(store) reconcile scans this engine has run at the hot-path call site. Without
    /// `flat_append()` this fires once per command (O(writes)); with it on the
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


/// How many times a sweep has moved pages out, for tests that need to know the bound fired
/// rather than that the count merely happened to stay low.
pub(crate) static RESIDENT_SWEEPS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// TS_WAL_RESIDENT_BLOCKS: how many log-resident blocks one shard may hold before the oldest are
/// written out to the block store. Zero disables the bound entirely. `TS_WAL_RESIDENT_PAGES` is
/// the previous spelling and is still read.
///
/// Resident blocks are bounded because an unbounded set costs twice. Each one is a registration,
/// which is memory; each one also pins `min_registered_sequence`, and reclaim may not truncate
/// below the lowest registration — so a set that only grows is a log that can never be reclaimed
/// whatever the retention policy says. Measured on an ingest of distinct keys, registrations
/// tracked writes ONE FOR ONE: 200 writes 200 held, 1200 writes 1200 held.
///
/// The default is deliberately generous. The recent ones are worth keeping where they are: a page
/// written moments ago is the one a read is most likely to want, and its bytes are already in the
/// record just written. This is a ceiling on how far behind the dump can fall, not a cache policy.
fn wal_resident_block_limit() -> usize {
    crate::env_flag::env_number_first(&["TS_WAL_RESIDENT_BLOCKS", "TS_WAL_RESIDENT_PAGES"], 4096)
}

/// How far below the limit a sweep goes, so a shard sitting exactly at the ceiling does not
/// materialise on every single append.
fn wal_resident_block_floor(limit: usize) -> usize {
    limit.saturating_sub(limit / 4).max(1)
}

impl TemporalEngine {
    /// Rebuild the bucket first-index once, for a caller that deferred it across a window.
    ///
    /// Only meaningful after an `IngestReconstructWindow` has held off the per-record rebuilds;
    /// calling it otherwise just repeats work the writes already did.
    pub(crate) fn reconstruct_bucket_index_now(&self, shard_id: ShardId) {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return;
        };
        storage_bucket_internals::rebuild_bucket_first_index(
            shard_id,
            shard,
            start_routing_bucket,
            end_routing_bucket,
        );
        // The rebuild above just recomputed every bucket's object index by scanning that bucket's
        // page index. The full sweep's own rebuild would scan the same pages again, from the same
        // source, with nothing in between that could change the answer -- so the flags refresh,
        // and the duplicate scan does not.
        storage_bucket_internals::refresh_bucket_runtime_flags_after_reconstruct(shard);
    }

    /// Take the shard table's write lock, marking the region for the lock-hold measurement.
    ///
    /// Identical to `self.shards.write()` in every observable way except that the served-index
    /// encode can tell whether it is running inside the region. Maintenance paths take this one
    /// so a guard can state what they do while holding it.
    fn shards_write_marked(&self) -> MarkedShardWriteGuard<'_> {
        MarkedShardWriteGuard::new(self.shards.write().expect("engine lock poisoned"))
    }

    /// Take the shard table's read lock, marking the region for the lock-hold measurement.
    ///
    /// Identical to `self.shards.read()` in every observable way except that the page-read
    /// counter can tell whether it is running inside the region.
    fn shards_read_marked(&self) -> MarkedShardReadGuard<'_> {
        MarkedShardReadGuard::new(self.shards.read().expect("engine lock poisoned"))
    }

    /// Read one live page off the block store, counted against the shard-table guard.
    ///
    /// The maintenance stages that read pages -- the cache warm-up and the recovery report's
    /// readable probe -- go through here so a guard can state whether the reads happened while
    /// the shard table was held. The count is taken here and the REGION is marked at the
    /// acquisition, so moving a read out of a guarded region moves it out of the under-guard
    /// tally without anything at this call site changing.
    pub(crate) fn read_block_counted(
        &self,
        address: &BlockAddress,
    ) -> Result<Vec<u8>, BlockStoreError> {
        shard_write_guard::note_block_read();
        self.block_store.read(address)
    }

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
        if let Some(sink) = self.maintenance_mirror_sink() {
            sink.record_write(shard_id, command);
        }
    }

    /// Look the mirror sink up ONCE, for a caller that is about to mirror a run of commands.
    ///
    /// The per-key form above takes the mirror lock and bumps an `Arc` refcount for every
    /// command, and the two callers that mirror a run of them -- the expiry sweep and the
    /// delete-drop eviction -- do it from inside the shard-table WRITE guard, so a round
    /// removing N keys took N+1 locks while holding the one lock that excludes everyone.
    ///
    /// Hoisting is not merely cheaper, it is more correct for a run: a sink swapped by
    /// `set_maintenance_wal_mirror` halfway through a loop would send some of one round's
    /// tombstones to the old sink and the rest to the new one, and neither would have the whole
    /// deletion. One lookup gives the whole run one destination.
    pub(crate) fn maintenance_mirror_sink(
        &self,
    ) -> Option<Arc<dyn crate::data_node::SharedWalSink>> {
        shard_write_guard::note_mirror_sink_lookup();
        self.maintenance_mirror
            .read()
            .expect("maintenance mirror lock poisoned")
            .clone()
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
    pub fn execute_with_carried_blocks(
        &self,
        request: ExecuteRequest,
        pages: Vec<crate::wal::StagedBlock>,
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
        self.execute_raft_apply_at(request, None)
    }

    /// `execute_raft_apply`, resolving time-dependent values against the instant the LEADER
    /// admitted the command rather than against this node's clock.
    ///
    /// A relative deadline becomes an absolute one while the command executes, and on this path
    /// the command executes once per replica at whatever moment that replica applied. Without the
    /// leader's instant, one committed entry produces as many absolute deadlines as there are
    /// replicas. `None` (or a zero stamp, from an entry written before the log carried one) keeps
    /// the live clock, which is exactly what this path did before.
    pub fn execute_raft_apply_at(
        &self,
        request: ExecuteRequest,
        leader_time_ms: Option<u64>,
    ) -> ExecuteResponse {
        let _guard = RaftApplyGuard::enter();
        let _clock = ReplayClockGuard::enter(leader_time_ms);
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
        self.execute_raft_apply_batch_at(
            requests.into_iter().map(|request| (request, None)).collect(),
        )
    }

    /// `execute_raft_apply_batch`, each entry carrying the instant its LEADER admitted it.
    ///
    /// The stamp is per ENTRY, not per batch: a batch is whatever run of committed entries this
    /// node happened to apply together, and those were admitted at different moments. Applying one
    /// batch-wide timestamp would replace a per-node drift with a per-batch one. The coalesced
    /// barrier is unaffected -- it is still taken once, after the loop.
    pub fn execute_raft_apply_batch_at(
        &self,
        requests: Vec<(ExecuteRequest, Option<u64>)>,
    ) -> Vec<ExecuteResponse> {
        if !self
            .raft_apply_coalesce
            .load(std::sync::atomic::Ordering::Relaxed)
            || requests.len() <= 1
        {
            return requests
                .into_iter()
                .map(|(request, leader_time_ms)| self.execute_raft_apply_at(request, leader_time_ms))
                .collect();
        }
        let _apply_guard = RaftApplyGuard::enter();
        let batch_guard = RaftApplyBatchGuard::enter();
        let mut responses = Vec::with_capacity(requests.len());
        for (request, leader_time_ms) in requests {
            let _clock = ReplayClockGuard::enter(leader_time_ms);
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
        mut carried_blocks: Vec<crate::wal::StagedBlock>,
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
            && !(self.wal_store.flat_append() && shard.promote_scan_done)
        {
            self.promote_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if promote_model_maps_to_bucket_index_authority(
                request.shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
            ) {
                reconcile_secondary_views_from_bucket_index(&self.block_store, shard, None);
            }
            // Mark the reconcile confirmed only once the shard actually holds model-map state:
            // `promote` returns false (without establishing anything) on an empty shard, so
            // guarding on non-emptiness avoids latching the flag before the first real write.
            if self.wal_store.flat_append() && shard_has_model_entries(shard) {
                shard.promote_scan_done = true;
            }
        }
        let write_command = is_write_command(&command);
        // Exempt for exactly the reason the token-bucket limit at the top of this function is
        // exempt, and the reason is not symmetry for its own sake. A follower that refuses what
        // its leader committed diverges from the leader. A replay that refuses a record already
        // in the log does not degrade the shard, it loses it: replay_wal_into_shard turns any
        // failed response into wal_replay_failed, and load_shard_with unwinds the shard and
        // refuses the load on it.
        //
        // Measured before this guard existed: a shard configured write_qps=3 applied 0 of 40
        // committed entries, and a replay of 40 records refused the 4th.
        if !raft_applying() && !replaying_wal() {
            if let Err(status) =
                self.check_admission(request.shard_id, write_command, &config, &info)
            {
                return ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                };
            }
        }
        // Exempt under raft apply and under replay, with the admission limits just above. The
        // ceiling reads the known physical bytes of the WHOLE store, which is not a number a
        // follower applying a committed entry, or a load rebuilding a shard, can do anything
        // about -- and a shard that cannot replay is not a shard held under a ceiling, it is a
        // shard that will not load, so it never reaches the reclamation that would clear it.
        if !raft_applying()
            && !replaying_wal()
            && write_command
            && config
                .maxmemory_bytes
                // Gate on the CURRENT on-disk footprint (decremented by GC/compaction/
                // purge), not the cumulative-ever `bytes_written` counter. The old gate
                // was a monotonic tripwire that reclamation could never clear, so a
                // long-running node permanently rejected all writes once it tripped.
                // compares live resident size and evicts; this at least lets
                // reclamation re-admit writes.
                .map(|limit| self.block_store.slab_summary().total_known_physical_bytes >= limit)
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
                &self.block_store,
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
        block_in_wal::begin_write();
        // What the touched keys held before this command, so the capture below can be
        // skipped when nothing was removed. Sizes and one `u64` -- no allocation.
        //
        // THE DEADLINE'S VALUE IS CARRIED SEPARATELY BECAUSE `key_membership_size` ONLY COUNTS
        // ITS PRESENCE. It adds `expires_at_ms.contains_key(key)` as one unit, which notices a
        // deadline being removed (a shrink) but not one being ARMED (a growth) or MOVED (no
        // change at all). Keeping the millisecond itself is what lets those two be seen. See
        // `delta_key_state_change` below.
        let membership_before: Vec<(String, usize, Option<u64>)> = command_object_keys(&command)
            .into_iter()
            .map(|key| {
                let size = key_membership_size(shard, &key);
                let deadline = shard.expires_at_ms.get(&key).copied();
                (key, size, deadline)
            })
            .collect();

        let outcome = execute_on_shard(
            &self.cache,
            &self.block_store,
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
                    block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                shard.bucket_recency.insert(recency_bucket, now);
            }
        }
        // Set to the reserved WAL sequence when the concurrent-commit path defers this
        // write's durable barrier out of the `shards` lock (TS_ENGINE_CONCURRENT_COMMIT).
        // The barrier is awaited AFTER the lock is released, just before the ack.
        let mut pending_barrier_seq: Option<u64> = None;
        // The sequence this write took, whichever append path assigned it. Separate from
        // `pending_barrier_seq`, which means "the barrier for this sequence is deferred" and is
        // set on only one of the two paths.
        let mut appended_sequence: Option<u64> = None;
        if outcome.mutated {
            let object_keys = command_object_keys(&command);
            // Capture this write's touched keys for the O(delta) index-log append below
            // (the command is moved into the WAL append before we reach that point).
            let delta_command_keys = object_keys.clone();
            // Captured here for the same reason as the keys above: the command is moved into
            // the WAL append before the index-log items are built, so anything the items need
            // from it has to be taken while it is still owned.
            let removed_component = command_removed_component(&command);
            let upsert_components = command_upsert_components(&command, shard);
            if object_keys.is_empty() {
                rebuild_bucket_block_ownership(
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
                    let start_routing_bucket = info
                        .as_ref()
                        .map(|info| info.start_routing_bucket)
                        .unwrap_or_default();
                    let end_routing_bucket = info
                        .as_ref()
                        .map(|info| info.end_routing_bucket)
                        .unwrap_or(u32::MAX);
                    // Recorded with its routing bucket. The synchronous branch below does not go
                    // through `mark_async_dirty_object`, so this is the other of the two sites
                    // that mark an object dirty and the bucket has to be supplied here too.
                    shard.dirty_objects.insert(
                        &object_key,
                        block_routing_bucket(
                            &object_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                    );
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
            // Maintain the index for the keys this write touched, and rebuild only if that did
            // not cover them.
            //
            // A context write does not register its page, so the branch below fired a full
            // O(store) rebuild for every one -- the last term in an add that grew with the corpus.
            // Feature and Sequence writes already maintain on the write path, and replay already
            // maintains these same kinds; this closes the one path that did neither.
            //
            // `sync_context_blocks_for_object` mirrors `collect_model_live_block_entries` arm for
            // arm and reports whether it found anything, so a write it does not cover still gets
            // the rebuild rather than a quietly stale index. An empty bucket_map still rebuilds:
            // maintenance updates an index, it does not construct one.
            let maintained_bucket_index = !shard.bucket_index.bucket_map.is_empty()
                && !delta_command_keys.is_empty()
                && delta_command_keys.iter().all(|object_key| {
                    storage_bucket_internals::sync_context_blocks_for_object(
                        shard,
                        request.shard_id,
                        object_key,
                    )
                });
            let rebuilt_bucket_index = !maintained_bucket_index
                && !defer_bucket_index_reconstruct()
                // A command that writes no page cannot have changed the page index, so rebuilding it
                // recomputes what it already held -- measured at twice the shard's page count per
                // call for SeenCheck and the control-state change/selection writes.
                && !command_writes_no_block(&command)
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
            // Drain what this execution recorded, on EVERY path -- not only the ones that go
            // on to append. Staging happens during execution, but the append below is guarded,
            // so any write that does not reach it left its items sitting in the thread's
            // buffer for whatever wrote next on that thread to adopt as its own. Replay is the
            // loud case: it re-executes commands with `replaying_wal()` true, stages an item
            // for everything it re-applies, and appends none of it -- so the first write after
            // a recovery inherited the recovery's items and recorded them as changes it had
            // made itself. A later replay then installs them, and if one cannot be applied it
            // aborts the whole shard load, taking unrelated keys down with it.
            // Nothing can be staged while recording is off, because the staging function is
            // where that is now decided -- so there is nothing left here to clear.
            let mut staged_outcomes = block_in_wal::take_outcomes();
            if write_command && !replaying_wal() {
                // A write that changed the shard and recorded nothing cannot be replaced by
                // its record. Every existing test that writes anything is a probe for that,
                // which covers far more of the mutating surface than a hand-listed fixture per
                // command would -- so this is checked in every debug build rather than behind
                // `TS_WAL_OUTCOME_STRICT`, which nothing ever set. Measured before making it
                // standing: 1628 lib tests, no violation. A release build pays nothing.
                //
                // Still conditional on records carrying results at all: with none there is
                // nothing for the check to be about.
                if crate::wal::wal_outcome_items_enabled()
                    && cfg!(debug_assertions)
                    && staged_outcomes.is_empty()
                {
                    let rendered = format!("{command:?}");
                    let label = rendered
                        .split_once(' ')
                        .map(|(head, _)| head.to_string())
                        .unwrap_or(rendered);
                    panic!(
                        "{label} changed shard {} and recorded nothing about what it did, so its record cannot replace it",
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
                    && carried_blocks.is_empty()
                    && (self
                        .concurrent_commit
                        .load(std::sync::atomic::Ordering::Relaxed)
                        || raft_apply_batch_active());
                // Where each page this write stages ends up, so the index can carry it. Filled
                // by the append below, which is the first moment the log id exists.
                let mut wal_resident_updates: Vec<(u64, crate::engine::state::WalResidentBlock)> =
                    Vec::new();
                let append_result = if concurrent_commit {
                    self.wal_store
                        .append_for_group_commit(
                            request.shard_id,
                            command,
                            std::mem::take(&mut staged_outcomes),
                            if carried_blocks.is_empty() {
                                block_in_wal::take_staged()
                            } else {
                                let _ = block_in_wal::take_staged();
                                std::mem::take(&mut carried_blocks)
                            },
                        )
                        .map(|record| {
                            appended_sequence = Some(record.sequence);
                            Some(record.sequence)
                        })
                } else {
                    self.wal_store
                        .append_with_outcomes(
                            request.shard_id,
                            command,
                            sync,
                            if carried_blocks.is_empty() {
                                block_in_wal::take_staged()
                            } else {
                                // The caller handed us the pages the original write produced.
                                // Drop whatever this execute re-derived rather than letting a
                                // reconstruction win over the bytes that were acked.
                                let _ = block_in_wal::take_staged();
                                std::mem::take(&mut carried_blocks)
                            },
                            std::mem::take(&mut staged_outcomes),
                        )
                        .map(|(record, log_id)| {
                            appended_sequence = Some(record.sequence);
                            // Point every page this record carries at the record, keyed on the
                            // object id the write derived -- which is what the stored address
                            // carries, so a read finds it by identity rather than by timing.
                            block_in_wal::register_record(
                                &self.block_store,
                                request.shard_id,
                                &record.staged_blocks,
                                log_id,
                                record.sequence,
                                &self.wal_store,
                            );
                            // Same fact, written down where it survives this process.
                            wal_resident_updates.extend(record.staged_blocks.iter().map(|page| {
                                (
                                    page.object_id,
                                    crate::engine::state::WalResidentBlock {
                                        log_id,
                                        sequence: record.sequence,
                                    },
                                )
                            }));
                            None
                        })
                };
                match append_result {
                    Ok(deferred_seq) => {
                        // The index carries where each staged page landed, so a reload can hand
                        // the mapping back rather than leaving the address unresolvable until a
                        // full replay re-derives the page.
                        for (object_id, placement) in wal_resident_updates.drain(..) {
                            shard.wal_resident_blocks.insert(object_id, placement);
                        }
                        // Record where this write sits in the log, for every bucket it dirtied
                        // that did not already have a claim.
                        //
                        // Done HERE rather than where the bucket is marked dirty, because the
                        // mark happens before the append and the sequence does not exist yet at
                        // that point. Both the sync and the async mark paths run above this, so
                        // one pass covers them.
                        //
                        // Only when unset: the field is the OLDEST undumped write, so a second
                        // write to an already-dirty bucket must not move it forward.
                        if let Some(sequence) = appended_sequence {
                            for key in &delta_command_keys {
                                let routing_bucket = block_routing_bucket(
                                    key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                );
                                if let Some(bucket) =
                                    shard.bucket_index.bucket_map.get_mut(&routing_bucket)
                                {
                                    if bucket.first_dirty_wal_sequence == 0 {
                                        bucket.first_dirty_wal_sequence = sequence;
                                    }
                                }
                            }
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
                        // would tell the client a write that is gone after a crash succeeded. The
                        // failed commit status is surfaced to the client instead of acking a write
                        // that is not durable, so the error is never swallowed. (Async/bulk mode is
                        // a fire-and-forget commit, so its append errors stay best-effort and do
                        // not fail the command.) We also skip the index anchor + persist below, so
                        // durable state never advances past a write the WAL did not accept.
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
                // flat append anchor off the O(1) cached last sequence (authoritative right after
                // this write's append) instead; without it the exact `stats()` value is kept.
                shard.applied_wal_sequence = Some(if self.wal_store.flat_append() || raft_apply_batch_active() {
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
                        collect_command_index_items_for(
                            shard,
                            &delta_command_keys,
                            start_routing_bucket,
                            end_routing_bucket,
                            // A removal that can name its component states that one page, not
                            // every page the object holds.
                            removed_component
                                .as_ref()
                                .map(|(kind, component)| (*kind, component.as_deref())),
                        ),
                        false,
                    ),
                };
                // Capture the authoritative per-key state only when this write produced some
                // that reconstruction from pages cannot redo: a membership SHRINK, or a
                // DEADLINE CHANGE. See `delta_key_state_change` for both halves and for why
                // the second one has to be asked separately.
                //
                // The capture exists so a reload after WAL replay does not resurrect an entry
                // the write evicted or tombstoned -- reconstruction from physical pages would
                // otherwise find it again. A write that only ADDED leaves nothing to resurrect:
                // replay rebuilds the same membership from the same pages.
                //
                // It is not free. Capturing serializes every entry the per-key maps hold for the
                // key, so appending to a node that held 850 events serialized all 850 -- 8,647 of
                // the 8,838 allocations a message write cost, and the reason filling a node cost
                // the square of its length.
                let key_state_changed = delta_key_state_change(shard, &membership_before);
                let key_states = if key_state_changed {
                    capture_key_states(shard, &delta_command_keys)
                } else {
                    Vec::new()
                };
                // `durable` fsyncs the delta record before returning. Deferred on the raft
                // apply path (raft log is the durability source) and, under the single-barrier
                // default, on the single-node path too: the record is still written (so the
                // served-index stream is unchanged), but the durable WAL barrier already
                // committed above makes the lost delta tail recoverable by base-only WAL replay,
                // so its fdatasync leaves the ack critical path. Restored to a synchronous
                // barrier only under the TS_WAL_LEGACY_RECOVERY escape hatch (wal_single_barrier
                // false -> delta-fold recovery, which trusts the durable delta).
                let index_log_durable = !raft_applying() && !wal_single_barrier();
                let appended_index_log_sequence = self
                    .index_log_store
                    .append_delta(
                        request.shard_id,
                        items,
                        key_states,
                        shard.applied_wal_sequence,
                        None,
                        upsert_record,
                        index_log_durable,
                    )
                    .unwrap_or(0);
                // The index-log half of what each dirtied bucket holds, stamped the same way as
                // the WAL half above and for the same reason: the reclaim plan keeps two
                // frontiers, counted in two different sequences, and a bucket that can place
                // itself in only one of them can hold neither.
                //
                // `append_delta` returns 0 when it wrote nothing (bulk ingest, or the log
                // disabled), which is exactly "no claim" -- so the guard is the same one.
                if appended_index_log_sequence > 0 {
                    for key in &delta_command_keys {
                        let routing_bucket =
                            block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                        if let Some(bucket) =
                            shard.bucket_index.bucket_map.get_mut(&routing_bucket)
                        {
                            if bucket.first_dirty_index_log_sequence == 0 {
                                bucket.first_dirty_index_log_sequence =
                                    appended_index_log_sequence;
                            }
                        }
                    }
                }
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
        // Locks are released and the barrier is done: a safe point to bound the resident set.
        // Not inside the write lock -- moving a page takes the same lock -- and not before the
        // barrier, because the write this call is acking must reach disk first.
        if write_command && !replaying_wal() {
            let limit = wal_resident_block_limit();
            if limit > 0
                && block_in_wal::registration_count(&self.block_store, request.shard_id) > limit
            {
                let moved = self.materialize_oldest_resident_blocks(
                    request.shard_id,
                    wal_resident_block_floor(limit),
                );
                if moved > 0 {
                    RESIDENT_SWEEPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
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
        // The addresses come from the shard index and the BYTES come from the block store, so
        // the guard covers the lookup only. A read guard admits other readers and excludes every
        // WRITER, and this path held it across one block-store read PER FIELD -- so a wide hash
        // stopped all writes on the shard for as many reads as the value had fields. See
        // `FastPathRead` for why releasing first is safe and what the one scope still buys.
        let hold_guard_across_reads = shard_write_guard::serving_holds_guard_across_reads();
        let (plan, _held_across_reads) = {
            let shards = self.shards_read_marked();
            let Some(shard) = shards.get(&request.shard_id) else {
                return Some(ExecuteResponse {
                    status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                    response: CommandResponse::Empty,
                });
            };
            let plan = match &request.command {
                Command::StringGet { key } => {
                    if shard
                        .expires_at_ms
                        .get(key)
                        .map(|expires_at| *expires_at <= resolve_now_ms())
                        .unwrap_or(false)
                    {
                        return None;
                    }
                    FastPathRead::String {
                        key: key.as_str(),
                        address: shard.strings.get(key).cloned(),
                    }
                }
                Command::HashGetAll { key } => {
                    if shard
                        .expires_at_ms
                        .get(key)
                        .map(|expires_at| *expires_at <= resolve_now_ms())
                        .unwrap_or(false)
                    {
                        return None;
                    }
                    FastPathRead::Hash {
                        fields: shard
                            .hashes
                            .get(key)
                            .map(|fields| {
                                fields
                                    .iter()
                                    .map(|(field, address)| (field.clone(), address.clone()))
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default(),
                    }
                }
                _ => return None,
            };
            // The measurement's control arm carries the guard out of this block so the reads
            // below still happen inside the region. `None` in every shipped build, which is what
            // makes the guard drop here.
            let held = if hold_guard_across_reads {
                Some(shards)
            } else {
                None
            };
            (plan, held)
        };

        match plan {
            FastPathRead::String { key, address } => Some(ExecuteResponse {
                status: Status::ok(),
                response: cached_response(
                    &self.cache,
                    CacheKey::string(request.shard_id, key),
                    || CommandResponse::Bytes {
                        value: address.as_ref().and_then(|address| {
                            read_block_bytes(
                                &self.cache,
                                &self.block_store,
                                request.shard_id,
                                address,
                            )
                        }),
                    },
                ),
            }),
            FastPathRead::Hash { fields } => {
                let mut entries = fields
                    .iter()
                    .filter_map(|(field, address)| {
                        read_block_bytes(&self.cache, &self.block_store, request.shard_id, address)
                            .map(|value| (field.clone(), value))
                    })
                    .collect::<Vec<_>>();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                Some(ExecuteResponse {
                    status: Status::ok(),
                    response: CommandResponse::HashEntries { entries },
                })
            }
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
                observed_local_file_read: stats.block_store_compat.reads > 0,
                observed_cache_invalidation: stats.cache.invalidations > 0,
                observed_memory_eviction: stats.cache.memory_evictions > 0,
                cache_memory_bytes: stats.cache.memory_bytes,
                cache_disk_bytes: stats.cache.disk_bytes,
                local_block_bytes_written: stats.block_store_compat.bytes_written,
                local_block_bytes_read: stats.block_store_compat.bytes_read,
                cache: stats.cache,
                block_store: stats.block_store_compat,
            })
    }

    #[doc(hidden)]
    pub fn string_block_cache_key_for_test(&self, shard_id: ShardId, key: &str) -> Option<CacheKey> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let address = shards.get(&shard_id)?.strings.get(key)?;
        Some(CacheKey::page_with_slot(
            shard_id,
            address.block_slab_id,
            address.offset,
            address.length,
            address.routing_bucket()))
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
        if let Some(manifest) = latest_bucket_dump_manifest_shared_at(&self.index_dir, shard_id) {
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
            if let Some(manifest) = latest_bucket_dump_manifest_shared_at(&self.index_dir, shard_id) {
                merge_last_dump_sequence(summaries, &manifest)
            } else {
                summaries
            };
        storage_physical_index_report(shard_id, shard, summaries)
    }

    pub fn bucket_object_block_ownership_report(
        &self,
        shard_id: ShardId,
    ) -> BucketObjectBlockOwnershipReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return BucketObjectBlockOwnershipReport {
                shard_id,
                ..BucketObjectBlockOwnershipReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        bucket_object_block_ownership_report(
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
        let ownership = self.bucket_object_block_ownership_report(shard_id);
        let object_manager = self.object_manager_runtime_report(shard_id);
        let slab_reports = self.block_store.slab_reports().unwrap_or_default();
        let block_index_count = slab_reports
            .iter()
            .map(|slab| slab.block_index_count)
            .sum::<u64>();
        // The checksum is asked for only when checksum recording is on. It is an opt-in
        // diagnostic (TS_BLOCK_INDEX_CHECKSUMS, default off, because recomputing it at every
        // engine open dominated slab verification), and the integrity it would attest to is
        // already verified by decode_block_record on the way in. Requiring it unconditionally
        // made this report ready: false on every default deployment. The addressing fields are
        // what a complete block address API means; the digest is a hand-inspection aid.
        let checksums_recorded = crate::block_store::block_index_checksums_enabled();
        // What a slab walk can know, which is what a block record carries: where the block is,
        // which block of its object it is, and its checksum. The object id and the routing
        // bucket are NOT among them any more -- they live in the index, and a record that
        // repeated them could only ever agree with the index or be wrong. Asking a slab-derived
        // entry for them would report the address API permanently incomplete for a reason that
        // is by design.
        let block_address_api_ready = slab_reports.iter().any(|slab| {
            slab.block_index_entries.iter().any(|entry| {
                entry.compact_slab_address.is_some()
                    && entry.compact_slab_id.is_some()
                    && entry.compact_slab_offset.is_some()
                    && entry.block_id.is_some()
                    && (!checksums_recorded || entry.checksum.is_some())
            })
        });
        let slab_report = self.block_store.stream_backed_slab_runtime_report().ok();
        let stream_backed_slab_api_ready = slab_report
            .as_ref()
            .map(|report| {
                report.slab_manifest_ready
                    && report.slab_manifest_disk_consistent
                    && report.slab_stats_ready
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
        if !stream_backed_slab_api_ready {
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
        let legacy_block_slab_aliases_ready = true;
        StorageDataStructureApiParityReport {
            shard_id,
            ready: blockers.is_empty() && legacy_block_slab_aliases_ready,
            bucket_object_block_authority_ready: physical_index.bucket_index_authority
                && ownership.first_class_index_present
                && !ownership.derived_from_model_maps,
            bucket_store_layout_api_ready,
            object_manager_runtime_api_ready: object_manager.runtime_ready,
            block_address_api_ready,
            block_store_slab_api_ready: block_index_count > 0,
            stream_backed_slab_api_ready,
            legacy_block_slab_aliases_ready,
            storage_manager_phase_api_ready,
            storage_manager_pressure_api_ready,
            storage_manager_merged_dump_load_api_ready,
            bucket_count: physical_index.bucket_count,
            page_index_count: physical_index.block_index_count,
            block_index_count,
            stream_slab_count: slab_report
                .as_ref()
                .map(|report| report.slab_count)
                .unwrap_or_default(),
            stream_record_count: slab_report
                .as_ref()
                .map(|report| report.stream_record_count)
                .unwrap_or_default(),
            storage_manager_stage_order: storage_manager.native_stage_order,
            blockers,
            evidence: vec![
                "slot/object/page authority is reported from the first-class slot index"
                    .to_string(),
                "block addresses expose segment, offset, length, block id, object id, routing slot and band id, plus a payload checksum when checksum recording is enabled"
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
        block_routing_bucket(key, start, end)
    }

    /// What the resident bucket index costs on this shard: one node per bucket, plus one entry
    /// per page the bucket holds.
    ///
    /// The published `bucket_index_resident_bytes_floor` counts NODES only. That is a floor and
    /// says so, but it also cannot move when a bucket is released -- the node is exactly what a
    /// release keeps. The per-page entries are what grows with the corpus and what a release
    /// frees, so this is the number to gate on and to assert against.
    pub fn bucket_index_resident_bytes(&self, shard_id: ShardId) -> u64 {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .get(&shard_id)
            .map(crate::engine::storage_bucket_internals::bucket_index_resident_bytes)
            .unwrap_or_default()
    }

    /// Release the named buckets' resident page lists, keeping each node routable and reloadable.
    ///
    /// Returns `(buckets released, pages released, candidates refused)`. Every precondition is
    /// checked against live state inside; naming a bucket that cannot be released is refused, not
    /// forced.
    pub fn release_bucket_index_blocks(
        &self,
        shard_id: ShardId,
        buckets: Vec<u32>,
    ) -> (usize, usize, usize) {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return (0, 0, 0);
        };
        let outcome = crate::engine::storage_bucket_internals::release_bucket_blocks(shard, &buckets);
        (
            outcome.released_buckets.len(),
            outcome.released_blocks,
            outcome.refused_buckets,
        )
    }

    /// Offer every bucket on the shard for release. The whole-shard form of the call above.
    pub fn release_all_releasable_bucket_index_blocks(
        &self,
        shard_id: ShardId,
    ) -> (usize, usize, usize) {
        let candidates = {
            let shards = self.shards.read().expect("engine lock poisoned");
            shards
                .get(&shard_id)
                .map(|shard| shard.bucket_index.bucket_map.keys().copied().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        self.release_bucket_index_blocks(shard_id, candidates)
    }

    /// Load a released bucket's page list back. False when the bucket was not released.
    pub fn reload_released_bucket_index_blocks(
        &self,
        shard_id: ShardId,
        routing_bucket: u32,
    ) -> bool {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return false;
        };
        crate::engine::storage_bucket_internals::reload_released_bucket(
            shard,
            shard_id,
            routing_bucket,
        )
    }

    /// The buckets currently released on this shard, in routing order.
    pub fn released_bucket_index_buckets(&self, shard_id: ShardId) -> Vec<u32> {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .get(&shard_id)
            .map(|shard| {
                shard
                    .bucket_index
                    .released_buckets
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
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

/// Whether this process is running a bulk backfill, which defers per-append durability work
/// to an explicit `sync_durable`.
///
/// The one reader of `MATRIXARK_BULK_INGEST`. block_store, index_log and wal each parsed it
/// themselves and gate their own half of the same decision; a copy that drifted would leave one
/// subsystem in bulk mode and the rest on the live path.
pub(crate) fn bulk_ingest_mode() -> bool {
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
/// object_page_lookup directly via upsert_bucket_index_block/read_bucket_index_value,
/// and the deferred context (model-map) records are append-only until the single
/// reconstruct folds them in (bulk: flush_shard_index(); replay: replay_wal_into_shard()).
fn defer_bucket_index_reconstruct() -> bool {
    bulk_ingest_mode() || replaying_wal() || coalescing_index_reconstruct()
}

/// Whether load_shard should eagerly warm the in-memory cache tier from the page
/// store after reconstructing the index (disk->memory promotion on restart).
/// Defaults ON; set MATRIXARK_EAGER_CACHE_WARM_ON_LOAD to 0/false/off/no to disable.
pub(crate) fn eager_cache_warm_on_load() -> bool {
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

/// What the serving read fast path decided to serve: chosen under the shard-table read guard,
/// read off the block store after it drops.
///
/// THE GUARD PROTECTS THE INDEX, NOT THE BYTES. Once an address is copied out, no writer can
/// invalidate it into something wrong: compaction relocates a block by APPENDING the new copy and
/// repointing the index, leaving the old bytes where they were, and slab ids are handed out
/// strictly monotonically, so a stale address never resolves to a DIFFERENT record. The one path
/// that destroys bytes quarantines the slab for `DELAYED_DESTROY_MIN_AGE_MS` first -- an hour,
/// documented as covering exactly "a reader holding a stale address" -- and a read that somehow
/// lost even that race fails the record's block-id and checksum check and answers ABSENT, which is
/// what a concurrently deleted key answers anyway.
///
/// What the single guard scope still buys, and what must not be narrowed away: a `HashGetAll`
/// snapshots EVERY field's address at one instant. Taking the guard once per field instead would
/// let a concurrent write land between two fields and serve half of one hash and half of another.
enum FastPathRead<'a> {
    String {
        key: &'a str,
        address: Option<BlockAddress>,
    },
    Hash {
        fields: Vec<(String, BlockAddress)>,
    },
}

/// What the served-index encode did while this thread held the shard-table WRITE guard.
///
/// The thing worth shortening is the HOLD, and on a shared box the hold cannot be timed: a build
/// running next door moves wall-clock by an order of magnitude, and both arms of a comparison do
/// not move by the same amount. What can be counted is the WORK done while the guard is held, and
/// on the maintenance paths the dominant item is the whole-index encode -- the shard's entire
/// served index through serde and then zstd, once per round, ahead of two file writes.
///
/// Everything here is THREAD-LOCAL, both the depth and the tallies. A process-global flag would
/// attribute another thread's guard to this one, and a process-global tally would attribute
/// another test's encode to this one; every test in this crate shares a process, and the tests
/// that drive concurrent writers share it with threads that are encoding for their own reasons.
/// Thread-local means a measurement means what it says however the suite is scheduled.
pub mod shard_write_guard {
    use std::cell::Cell;

    thread_local! {
        /// How many marked shard-table write guards this thread currently holds.
        static DEPTH: Cell<u32> = const { Cell::new(0) };
        static ENCODES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };
        static ENCODE_BYTES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };
        static ENCODES_TOTAL: Cell<u64> = const { Cell::new(0) };
        static ENCODE_BYTES_TOTAL: Cell<u64> = const { Cell::new(0) };
    }

    /// Served-index encodes on THIS thread since the last reset.
    ///
    /// The `_total` pair is the denominator. An assertion that nothing encoded under the guard is
    /// satisfied just as well by a path that did not encode at all -- or did not run -- so the
    /// totals have to be reported beside it and checked.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct IndexEncodeCounts {
        pub encodes_under_guard: u64,
        pub encode_bytes_under_guard: u64,
        pub encodes_total: u64,
        pub encode_bytes_total: u64,
    }

    pub(super) fn entered() {
        DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    }

    pub(super) fn left() {
        DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }

    /// Whether this thread is inside a marked shard-table write-guard region.
    pub(super) fn held() -> bool {
        DEPTH.with(|depth| depth.get() > 0)
    }

    pub(super) fn note_index_encode(bytes: usize) {
        ENCODES_TOTAL.with(|count| count.set(count.get().saturating_add(1)));
        ENCODE_BYTES_TOTAL.with(|total| total.set(total.get().saturating_add(bytes as u64)));
        if held() {
            ENCODES_UNDER_GUARD.with(|count| count.set(count.get().saturating_add(1)));
            ENCODE_BYTES_UNDER_GUARD
                .with(|total| total.set(total.get().saturating_add(bytes as u64)));
        }
    }

    pub fn index_encode_counts() -> IndexEncodeCounts {
        IndexEncodeCounts {
            encodes_under_guard: ENCODES_UNDER_GUARD.with(|count| count.get()),
            encode_bytes_under_guard: ENCODE_BYTES_UNDER_GUARD.with(|total| total.get()),
            encodes_total: ENCODES_TOTAL.with(|count| count.get()),
            encode_bytes_total: ENCODE_BYTES_TOTAL.with(|total| total.get()),
        }
    }

    /// Clear this thread's tallies. For a test measuring one operation.
    pub fn reset_index_encode_counts() {
        ENCODES_UNDER_GUARD.with(|count| count.set(0));
        ENCODE_BYTES_UNDER_GUARD.with(|total| total.set(0));
        ENCODES_TOTAL.with(|count| count.set(0));
        ENCODE_BYTES_TOTAL.with(|total| total.set(0));
    }

    thread_local! {
        /// How many marked shard-table READ guards this thread currently holds.
        ///
        /// Kept apart from the write depth because the two answer different questions. A write
        /// guard excludes everyone; a read guard admits other readers and excludes only writers.
        /// A maintenance stage that reads pages while holding this one is not blocking other
        /// reads -- it is blocking every WRITE on the shard, including the rest of its own round.
        static READ_DEPTH: Cell<u32> = const { Cell::new(0) };
        static BLOCK_READS_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };
        static BLOCK_READS_TOTAL: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn entered_read() {
        READ_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    }

    pub(super) fn left_read() {
        READ_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }

    /// Whether this thread is inside a marked shard-table region of EITHER kind.
    fn held_any() -> bool {
        held() || READ_DEPTH.with(|depth| depth.get() > 0)
    }

    /// The same, for counters outside this module. A manifest file read or write taken while
    /// this is true blocks every write on the shard for the duration of the syscall.
    pub(crate) fn any_shard_guard_held() -> bool {
        held_any()
    }

    /// Page-store reads the maintenance paths performed, and how many of them were performed
    /// while this thread held a shard-table guard.
    ///
    /// Same reasoning as the encode counters above, applied to the other thing a maintenance
    /// stage does that scales with the STORE rather than with the round's budget: reading live
    /// pages off the block store. The `_total` is the denominator -- "no reads happened under a
    /// guard" is satisfied just as well by a stage that read nothing, or did not run.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct MaintenanceBlockReadCounts {
        pub block_reads_under_guard: u64,
        pub block_reads_total: u64,
    }

    thread_local! {
        /// Whether the serving read fast path should keep the shard-table read guard alive
        /// across its page reads, the way it did before they were moved out.
        ///
        /// Kept for the reason the other control arms are kept: a guard asserting ZERO reads
        /// under the lock is vacuous unless an arm in the same process, on the same fixture,
        /// can still produce a non-zero one. Thread-local so one arm cannot leak into another.
        static SERVE_UNDER_SHARD_GUARD: Cell<bool> = const { Cell::new(false) };
    }

    /// Serve the read fast path with the guard held across the reads, for the measurement.
    #[cfg(test)]
    pub fn serve_fast_path_under_shard_guard_for_test(hold: bool) {
        SERVE_UNDER_SHARD_GUARD.with(|flag| flag.set(hold));
    }

    #[cfg(test)]
    pub(super) fn serving_holds_guard_across_reads() -> bool {
        SERVE_UNDER_SHARD_GUARD.with(|flag| flag.get())
    }

    /// Always false outside tests: the shipped path reads its pages after the guard drops.
    #[cfg(not(test))]
    pub(super) fn serving_holds_guard_across_reads() -> bool {
        false
    }

    pub(super) fn note_block_read() {
        BLOCK_READS_TOTAL.with(|count| count.set(count.get().saturating_add(1)));
        if held_any() {
            BLOCK_READS_UNDER_GUARD.with(|count| count.set(count.get().saturating_add(1)));
        }
    }

    pub fn maintenance_block_read_counts() -> MaintenanceBlockReadCounts {
        MaintenanceBlockReadCounts {
            block_reads_under_guard: BLOCK_READS_UNDER_GUARD.with(|count| count.get()),
            block_reads_total: BLOCK_READS_TOTAL.with(|count| count.get()),
        }
    }

    /// Clear this thread's page-read tallies. For a test measuring one stage.
    pub fn reset_maintenance_block_read_counts() {
        BLOCK_READS_UNDER_GUARD.with(|count| count.set(0));
        BLOCK_READS_TOTAL.with(|count| count.set(0));
    }

    thread_local! {
        /// Times this thread took the maintenance-mirror lock to look the sink up.
        ///
        /// A different shape of cost from the two above: not one big thing under the guard but a
        /// small one repeated per item. A round that deletes N keys can take this lock once or N
        /// times, and only a count distinguishes them -- N uncontended acquisitions are
        /// invisible to wall-clock on a loaded box and exactly as invisible when they are gone.
        static MIRROR_SINK_LOOKUPS: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn note_mirror_sink_lookup() {
        MIRROR_SINK_LOOKUPS.with(|count| count.set(count.get().saturating_add(1)));
    }

    /// Maintenance-mirror lock acquisitions on this thread since the last reset.
    pub fn maintenance_mirror_sink_lookups() -> u64 {
        MIRROR_SINK_LOOKUPS.with(|count| count.get())
    }

    /// Clear this thread's mirror-lookup tally. For a test measuring one round.
    pub fn reset_maintenance_mirror_sink_lookups() {
        MIRROR_SINK_LOOKUPS.with(|count| count.set(0));
    }
}

/// The shard table's write guard, with the region it covers marked for measurement.
///
/// Marking at the ACQUISITION rather than around each expensive call is what makes the counter
/// hard to blind: a call that moves into the region starts being counted because it is in the
/// region, not because someone remembered to wrap it, and a call that moves out stops for the
/// same reason. The mark is released by the same `Drop` that releases the lock, so an early
/// `drop(guard)` shortens the measured region exactly as much as it shortens the real one.
struct MarkedShardWriteGuard<'a> {
    guard: std::sync::RwLockWriteGuard<'a, HashMap<ShardId, ShardState>>,
}

impl<'a> MarkedShardWriteGuard<'a> {
    fn new(guard: std::sync::RwLockWriteGuard<'a, HashMap<ShardId, ShardState>>) -> Self {
        shard_write_guard::entered();
        Self { guard }
    }
}

impl std::ops::Deref for MarkedShardWriteGuard<'_> {
    type Target = HashMap<ShardId, ShardState>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for MarkedShardWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for MarkedShardWriteGuard<'_> {
    fn drop(&mut self) {
        shard_write_guard::left();
    }
}

/// The shard table's READ guard, with the region it covers marked the same way.
///
/// A read guard looks harmless and is not: it admits other readers but excludes every writer, so
/// a maintenance stage holding one across per-page I/O stops all writes on the shard for as long
/// as the I/O takes. Marked at the acquisition for the same reason the write guard is -- work
/// moving out of the region stops being counted because of WHERE IT IS, not because a wrapper
/// was remembered.
struct MarkedShardReadGuard<'a> {
    guard: std::sync::RwLockReadGuard<'a, HashMap<ShardId, ShardState>>,
}

impl<'a> MarkedShardReadGuard<'a> {
    fn new(guard: std::sync::RwLockReadGuard<'a, HashMap<ShardId, ShardState>>) -> Self {
        shard_write_guard::entered_read();
        Self { guard }
    }
}

impl std::ops::Deref for MarkedShardReadGuard<'_> {
    type Target = HashMap<ShardId, ShardState>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl Drop for MarkedShardReadGuard<'_> {
    fn drop(&mut self) {
        shard_write_guard::left_read();
    }
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
    let bytes = encode_index_bytes(shard);
    shard_write_guard::note_index_encode(bytes.len());
    bytes
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    let bytes = if shard.index_format_version == SHARD_INDEX_FORMAT_VERSION {
        encode_index_bytes(shard)
    } else {
        wrap_index_json(
            serde_json::to_vec(&stamp_index_format_version(shard))
                .expect("shard index should serialize"),
        )
    };
    // Counted at the two production entry points rather than inside `encode_index_bytes`, so the
    // tally is one per served-index encode whichever branch produced it -- and so the handful of
    // tests that call the encoder directly to check a container shape do not register as engine
    // work that some path did.
    shard_write_guard::note_index_encode(bytes.len());
    bytes
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

/// Does this look like a served index, in either of the two formats a reader may be handed?
///
/// A JSON index starts with `{`; a container starts with its magic. Callers that only need to
/// know "these bytes are an index" -- rather than to decode one -- ask this instead of parsing.
pub(crate) fn bytes_look_like_served_index(bytes: &[u8]) -> bool {
    bytes.first() == Some(&b'{') || bytes.starts_with(INDEX_CONTAINER_MAGIC)
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

/// The single place the served index becomes bytes, and it always writes the container.
///
/// The measured cost of raw JSON here is real: a 1 000-memory store carries a 74 MB index, and a
/// dump rewrites it whole. Compressing the same JSON keeps ONE representation of the struct --
/// no schema mirrored by hand, no second definition to drift -- while cutting what is written and
/// what must be read back at load.
///
///
/// Reading is unconditional and sniffed, so this flag only ever controls what is WRITTEN, and an
/// index written either way loads in either setting.
///
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
/// rather than on the table above. Measured over 120 memories per case, comparing full retrieval
/// snapshots across a restart:
///
///   * written by the container, reopened by it -- identical.
///   * written as JSON, reopened with the container on -- identical, and the index on disk becomes
///     a container, so an existing store upgrades in place with no migration step.
///
/// A third case -- a container-written index reopened with raw-JSON writing selected -- measured
/// identical too, and being reversible in both directions is what made the container safe to adopt
/// as a default rather than an opt-in. That switch has since been removed, so only the two cases
/// above are reachable now.
///
/// A reader never has to be told which it is holding: JSON starts with `{`, a container with its
/// magic, so both formats stay loadable.
///
/// Writing raw JSON was a switch until nothing selected it: reading is sniffed either way, so the
/// only thing the off position produced was an index an older build could read, and producing one
/// now means running such a build. `encode_index_bytes_as_plain_json` keeps that shape reachable
/// from tests, which is where the claim "both shapes still load" has to be proved.
pub(super) fn encode_index_bytes(shard: &ShardState) -> Vec<u8> {
    if index_container_codec() == INDEX_CODEC_ZSTD_MSGPACK {
        // A binary payload only works from the struct itself, so the version-stamping path (which
        // patches a `Value`) keeps to JSON; both still land inside the same container.
        if let Some(encoded) = encode_index_msgpack(shard) {
            return encoded;
        }
    }
    wrap_index_json(serde_json::to_vec(shard).expect("shard index should serialize"))
}

/// Put already-serialized index JSON into the container. Separate from `encode_index_bytes`
/// because the version-stamping path serializes a patched `Value` rather than the struct, and both
/// must produce the same on-disk shape.
fn wrap_index_json(json: Vec<u8>) -> Vec<u8> {
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

/// The served index as an older build wrote it: the struct's JSON, in no container.
///
/// Production has no way to produce this any more, and that is the point -- but the reader still
/// takes it, and a claim about what a reader accepts is worth only as much as the bytes used to
/// test it. This is those bytes.
#[cfg(test)]
pub(super) fn encode_index_bytes_as_plain_json(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec(shard).expect("shard index should serialize")
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
fn command_upsert_components(
    command: &Command,
    shard: &ShardState,
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
        // A zset add writes exactly one component, and it is derivable from the command. Declaring
        // it takes the O(1) delta path; without it the record snapshots every page of the object,
        // which cost 4 allocations per member already present.
        Command::ZSetAdd { key, member, score } => Some(vec![(
            "zset",
            key.clone(),
            Some(crate::engine::execute_on_shard::zset_component(
                crate::engine::execute_on_shard::zset_score_bits(*score),
                member,
            )),
        )]),
        Command::SetAdd { key, member } => {
            Some(vec![("set", key.clone(), Some(hex::encode(member)))])
        }
        // A push files its page under its sequence number, which is only knowable once the
        // write has landed: post-apply the pushed element is the list's FIRST entry for a left
        // push and its LAST for a right one. That is why this needs shard state and the other
        // arms do not.
        Command::ListPush { key, left, .. } => {
            let seq = shard.lists.get(key).and_then(|list| {
                if *left {
                    list.keys().next().copied()
                } else {
                    list.keys().next_back().copied()
                }
            })?;
            Some(vec![(
                "list",
                key.clone(),
                Some(format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64))),
            )])
        }
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
            // `zset_component` is `{biased:016x}` followed by hex(member), so the member the map is
            // keyed by is recoverable from the component it was filed under.
            ("zset", Some(component)) => component
                .get(16..)
                .and_then(|member_hex| hex::decode(member_hex).ok())
                .and_then(|member| {
                    shard
                        .zsets
                        .get(object_key)
                        .and_then(|members| members.get(&member))
                        .map(|(_, address)| address.clone())
                }),
            // `hex::encode(member)` is the component a set add files its page under, so
            // the member the map is keyed by is recoverable from the component itself.
            // `{biased:016x}` of the entry's sequence, so the key the list map is keyed by
            // is recoverable from the component it was filed under.
            ("list", Some(component)) => u64::from_str_radix(component, 16)
                .ok()
                .map(|biased| biased.wrapping_add(i64::MIN as u64) as i64)
                .and_then(|seq| {
                    shard
                        .lists
                        .get(object_key)
                        .and_then(|entries| entries.get(&seq))
                        .cloned()
                }),
            ("set", Some(component)) => hex::decode(component)
                .ok()
                .and_then(|member| {
                    shard
                        .sets
                        .get(object_key)
                        .and_then(|members| members.get(&member))
                        .cloned()
                }),
            _ => None,
        };
        let Some(address) = address else { continue };
        let routing_bucket = address
            .routing_bucket()
            .unwrap_or_else(|| {
                block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket)
            });
        let object_id = address.object_id().unwrap_or_else(|| {
            stable_block_object_id(shard_id, kind, object_key, component.as_deref())
        });
        let block_ref_key = format!(
            "{}:{}:{}:{}:{}:{}:{}:{}",
            kind,
            object_key,
            component.as_deref().unwrap_or(""),
            address.block_slab_id,
            address.offset,
            address.length,
            address.block_id().unwrap_or_default(),
            address.generation().unwrap_or_default()
        );
        items.push(crate::index_log::IndexItem {
            kind: crate::index_log::IndexItemKind::Page,
            routing_bucket,
            block_ref_key,
            object_key: object_key.clone(),
            model_id: (*kind).to_string(),
            component: component.clone(),
            object_id,
            block_id: address.block_id().unwrap_or(0),
            size: address.length,
            in_log: address.block_id().is_none(),
            deleted: false,
            address: Some(address),
        });
    }
    items
}

/// The (kind, component) a typed removal deleted, when the command names it outright.
///
/// Only commands whose component is derivable from their own fields appear here. ZSetRemove's
/// component folds in the score it is deleting, and ListPop's is the sequence it just took --
/// neither survives the operation, so neither can be named from the command afterwards. Those
/// keep restating the whole object until the component is carried out of the arm.
fn command_removed_component(command: &Command) -> Option<(&'static str, Option<String>)> {
    match command {
        Command::SetRemove { member, .. } => Some(("set", Some(hex::encode(member)))),
        Command::HashDelete { field, .. } => Some(("hash", Some(field.clone()))),
        _ => None,
    }
}

fn collect_command_index_items(
    shard: &ShardState,
    command_keys: &[String],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> Vec<crate::index_log::IndexItem> {
    collect_command_index_items_for(shard, command_keys, start_routing_bucket, end_routing_bucket, None)
}

/// The same, restricted to one component when the command can name what it touched.
///
/// Without a filter this states EVERY page the object holds, and each item costs four string
/// allocations -- so a removal from a 3,200-member collection built 3,200 items to record that
/// one member went away, measured at ~2.5 MB per call. A command that can name its component
/// states that one page instead, which is what the add path already does through
/// `command_upsert_components`.
///
/// The page is still found: a typed removal marks its page deleted in the bucket index rather
/// than dropping it, so the item this emits carries `deleted: true` and the deletion is recorded
/// exactly as before -- the guarantee that keeps a replay from resurrecting it.
fn collect_command_index_items_for(
    shard: &ShardState,
    command_keys: &[String],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    only: Option<(&str, Option<&str>)>,
) -> Vec<crate::index_log::IndexItem> {
    use std::collections::BTreeSet;
    let keys: BTreeSet<&str> = command_keys.iter().map(String::as_str).collect();
    if keys.is_empty() {
        return Vec::new();
    }
    let buckets: BTreeSet<u32> = keys
        .iter()
        .map(|key| block_routing_bucket(key, start_routing_bucket, end_routing_bucket))
        .collect();
    let mut items = Vec::new();
    for routing_bucket in buckets {
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            continue;
        };
        for (block_ref_key, page) in &bucket.block_index {
            if !keys.contains(page.object_key.as_ref()) {
                continue;
            }
            if let Some((kind, component)) = only {
                if &*page.model_id != kind || page.component.as_deref() != component {
                    continue;
                }
            }
            items.push(crate::index_log::IndexItem {
                kind: crate::index_log::IndexItemKind::Page,
                routing_bucket,
                block_ref_key: block_ref_key.to_string(),
                object_key: page.object_key.clone().to_string(),
                model_id: page.model_id.clone().to_string(),
                component: page.component.clone().map(|value| value.to_string()),
                object_id: page.object_id(),
                block_id: page.address.block_id().unwrap_or(0),
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
/// Each blob is `{"key": ..., "<map>": <value>}`, carrying ONLY the maps the key is actually
/// in. A field left out means the same thing an explicit null used to: the key is absent from
/// that map, a tombstone. `apply_key_state_field` cannot tell the two apart -- `None` and
/// `Some(null)` take the same `remove_entry` arm -- so the null was never read, and it cost its
/// own field name on every record for the life of the log.
///
/// That was most of the record. A write touching one ordinary key is in none of these thirteen
/// maps, so every one was written as a null: measured on a 100,000-record store, the always-null
/// fields were 308 bytes of a 656-byte index-log record.
///
/// Opaque JSON so the index-log layer stays decoupled from the concrete `ShardState` field types.
/// How many entries the per-key maps hold for `key`, added up.
///
/// Counting, not capturing: thirteen `len()` calls and no allocation, against serializing every
/// entry those maps hold for the key. Used to decide whether the capture below is needed at all.
fn key_membership_size(shard: &ShardState, key: &str) -> usize {
    shard.features.get(key).map_or(0, |v| v.len())
        + usize::from(shard.expires_at_ms.contains_key(key))
        + shard.control_state_changes.get(key).map_or(0, |v| v.len())
        + usize::from(shard.control_state_selection.contains_key(key))
        + shard.context_nodes.get(key).map_or(0, |_| 1)
        + shard.context_events.get(key).map_or(0, |v| v.len())
        + shard.context_indexes.get(key).map_or(0, |v| v.len())
        + shard.context_audits.get(key).map_or(0, |v| v.len())
        + shard.context_children.get(key).map_or(0, |v| v.len())
        + shard.context_summaries.get(key).map_or(0, |v| v.len())
        + shard.context_compressions.get(key).map_or(0, |v| v.len())
        + shard.context_entities.get(key).map_or(0, |v| v.len())
}

/// Did this write produce per-key state that reconstruction from physical pages cannot redo?
///
/// TWO REASONS, AND THE SECOND ONE COVERS THE HALF THE FIRST CANNOT SEE.
///
/// The first is a MEMBERSHIP SHRINK: an entry was evicted or tombstoned, and rebuilding the
/// index from the pages on disk would find it again and resurrect it. That is what this used
/// to ask on its own, and it is still asked first because it is the cheap half.
///
/// The second is a DEADLINE CHANGE. `key_membership_size` DOES count the deadline -- but only
/// as `expires_at_ms.contains_key(key)`, one unit of presence. That is enough to notice a
/// deadline being REMOVED (1 -> 0 is a shrink, so `CommonPersist` and `SET` without `KEEPTTL`
/// were always captured) and blind to the other two directions:
///
///   * ARMING a deadline where there was none is 0 -> 1, a GROWTH, and the gate only fires on
///     a shrink;
///   * MOVING a deadline to a different millisecond leaves `contains_key` true on both sides,
///     so the size does not move at all.
///
/// Either of those left `key_states` empty while the delta record it rode on still advanced
/// `applied_wal_sequence` to cover the WAL entry that set the deadline. On the legacy-recovery
/// load path the fold trusts that anchor and replays only the WAL tail BEYOND it, so the
/// command that armed the deadline is replayed by nobody, and the deadline is recovered from
/// neither the base, nor the record, nor the log. The anchor advance is what makes this a loss
/// rather than a slow path.
///
/// WHY THIS DOES NOT REINTRODUCE THE CAPTURE COST. Capturing serializes every entry the
/// per-key maps hold, which is why it is gated at all -- on a real corpus the unconditional
/// version wrote 3.53 GB of index log against 16 MB for the same 20,001 messages. That cost
/// came from APPENDS to long postings, and an append does not move a deadline, so it still
/// takes the cheap path. What newly captures is a write that armed or moved a key's deadline,
/// which is a deliberate and far rarer act than appending to a node.
///
/// The comparison is on `Option<u64>`, so a re-arm to the SAME millisecond correctly is not a
/// change and does not capture.
fn delta_key_state_change(
    shard: &ShardState,
    membership_before: &[(String, usize, Option<u64>)],
) -> bool {
    membership_before
        .iter()
        .any(|(key, size_before, deadline_before)| {
            key_membership_size(shard, key) < *size_before
                || shard.expires_at_ms.get(key).copied() != *deadline_before
        })
}

fn capture_key_states(shard: &ShardState, keys: &[String]) -> Vec<serde_json::Value> {
    keys.iter()
        .map(|key| {
            let mut blob = serde_json::Map::new();
            blob.insert("key".to_string(), serde_json::Value::String(key.clone()));
            let mut put = |name: &str, value: Option<serde_json::Value>| {
                if let Some(value) = value {
                    if !value.is_null() {
                        blob.insert(name.to_string(), value);
                    }
                }
            };
            put("features", shard.features.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("expires_at_ms", shard.expires_at_ms.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("control_state_changes", shard.control_state_changes.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("control_state_change_sketch", shard.control_state_change_sketch.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("control_state_selection", shard.control_state_selection.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_nodes", shard.context_nodes.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_events", shard.context_events.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_indexes", shard.context_indexes.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_audits", shard.context_audits.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_children", shard.context_children.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_summaries", shard.context_summaries.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_compressions", shard.context_compressions.get(key).and_then(|v| serde_json::to_value(v).ok()));
            put("context_entities", shard.context_entities.get(key).and_then(|v| serde_json::to_value(v).ok()));
            serde_json::Value::Object(blob)
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
        // DIRECT WRITE TO `expires_at_ms` -- the deadline-ordered mirror is invalidated at the
        // end of this function. See the note there.
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
    if !key_states.is_empty() {
        // THE ONE PLACE `expires_at_ms` IS WRITTEN WITHOUT `set_expiry` / `clear_expiry`.
        //
        // Those two keep the deadline-ordered mirror `expiry_by_deadline` in step entry by entry.
        // This cannot: it restores a whole captured map for each key, and an ABSENT
        // `expires_at_ms` field means "this key had no deadline", which `apply_key_state_field`
        // turns into a removal -- so both directions are reachable, and neither is visible from
        // the blob without re-deriving it.
        //
        // So the mirror is dropped instead and `ensure_expiry_order` rebuilds it from the
        // corrected map on first use. Dropping is not merely the cheap option, it is the only one
        // that works: `ensure_expiry_order` repairs ONLY an entirely empty mirror, so a mirror
        // left populated and wrong is never repaired and the keys it misses silently never expire.
        //
        // Today this is a no-op -- the delta fold only ever runs on a freshly decoded state, whose
        // mirror is already empty because the field is `#[serde(skip)]`. It is written down
        // because that is a property of the CALLER, not of this function, and the first caller
        // that folds a delta onto a shard already in service would land exactly on the
        // never-repaired case.
        shard.expiry_by_deadline.clear();
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
fn fold_delta_block_items(
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
            bucket.block_index.retain(&mut bucket_index.block_slab_live, |_, page| {
                !(page.model_id.as_ref() == item.model_id
                    && page.object_key.as_ref() == item.object_key.as_str()
                    && page.component.as_deref() == item.component.as_deref())
            });
        }
    } else if !covered_keys.is_empty() {
        let CoreIndex {
            bucket_map,
            block_slab_live: live,
            ..
        } = &mut *bucket_index;
        for bucket in bucket_map.values_mut() {
            bucket
                .block_index
                .retain(live, |_, page| !covered_keys.contains(page.object_key.as_ref()));
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
        // The record's key is not carried into memory: the map assigns a handle, and the
        // record's spelling is only rebuilt when the index is written back out.
        bucket.block_index.insert(
            BlockIndex {
                object_key: Arc::from(item.object_key.clone()),
                model_id: Arc::from(item.model_id.clone()),
                component: item.component.clone().map(Arc::from),
                address: {
                    // The record carries the id separately; the address holds it now.
                    let mut address = address;
                    address.set_object_id(Some(item.object_id));
                    address
                },
                dirty: false,
                deleted: false,
                log_backed: item.in_log,
            },
            &mut bucket_index.block_slab_live,
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

/// Holds the replay clock for the span of ONE apply and restores whatever was there before.
///
/// The clock is a thread-local, and a raft apply runs on a pooled thread that goes straight back
/// to serving live commands. Setting it without restoring would leave the NEXT command on that
/// thread resolving its deadlines against a committed entry's timestamp -- a leak that widens with
/// every apply and shows up as deadlines in the past. Restoring on drop also survives a panic
/// inside the apply.
pub(super) struct ReplayClockGuard(Option<u64>);

impl ReplayClockGuard {
    /// `None`, and a zero timestamp, both mean "unstamped": leave the live clock in charge.
    pub(super) fn enter(clock_ms: Option<u64>) -> Self {
        let previous = REPLAY_CLOCK_MS.with(|cell| cell.get());
        set_replay_clock_ms(clock_ms.filter(|stamp| *stamp > 0).or(previous));
        Self(previous)
    }
}

impl Drop for ReplayClockGuard {
    fn drop(&mut self) {
        set_replay_clock_ms(self.0);
    }
}

/// Wall-clock time for deadline / event-time stamping. Returns the replay clock (the
/// leader timestamp of the record being replayed) when set, otherwise the live clock.
pub(super) fn resolve_now_ms() -> u64 {
    REPLAY_CLOCK_MS
        .with(|cell| cell.get())
        .unwrap_or_else(now_ms)
}

thread_local! {
    static COALESCING_INDEX_RECONSTRUCT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn coalescing_index_reconstruct() -> bool {
    COALESCING_INDEX_RECONSTRUCT.with(|cell| cell.get())
}

/// Hold the per-record index reconstruct for the span of ONE logical ingest.
///
/// A context ingest issues about eight writes, and no `Command::Context*` appears in
/// `command_updates_bucket_index_directly`, so each one fires `rebuild_bucket_first_index` --
/// which walks every live page in the shard. Measured end to end: 5 762 400 page visits across
/// 600 adds, the per-add cost doubling as the corpus doubles, and latency degrading 11.79x over a
/// run. With the reconstruct deferred the same run is FLAT at 0.97x and 26.6x faster on the last
/// thirty adds.
///
/// This is the same mechanism bulk backfill and WAL replay already use, scoped to one ingest
/// instead of a whole session: the window defers, and the caller reconstructs ONCE as it closes.
/// It cuts eight reconstructs to one. It does NOT make an add O(1) in the corpus -- the single
/// remaining reconstruct still walks the store -- and that needs the context write path to
/// maintain the index the way `StringSet` does.
///
/// A guard rather than a pair of calls so an early return cannot leave the window open: a leaked
/// window would silently stop reconstructing for the rest of the thread's life.
pub(crate) struct IngestReconstructWindow;

impl IngestReconstructWindow {
    pub(crate) fn open() -> Self {
        COALESCING_INDEX_RECONSTRUCT.with(|cell| cell.set(true));
        Self
    }
}

impl Drop for IngestReconstructWindow {
    fn drop(&mut self) {
        COALESCING_INDEX_RECONSTRUCT.with(|cell| cell.set(false));
    }
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
pub(crate) fn wal_legacy_recovery() -> bool {
    env_flag_on("TS_WAL_LEGACY_RECOVERY")
}

/// On the write/ack path only the WAL takes a synchronous durability barrier; the served-index
/// checkpoint write is issued but its fsync is deferred to the background flush / OS writeback
/// (reconstructable from the WAL on recovery). Implied by the single-barrier default; disabled
/// only by the TS_WAL_LEGACY_RECOVERY escape hatch.
pub(super) fn wal_only_sync() -> bool {
    wal_single_barrier()
}

/// The true SINGLE write-path durability barrier. Only the WAL takes a
/// synchronous fdatasync per write (1.00/write); the data-page fdatasync, the served-index
/// delta-log fdatasync, the slab-manifest persist, and the base-index sync are all deferred.
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
///
/// This block used to open by naming `TS_WAL_SINGLE_BARRIER`, which is read by nothing -- the
/// only variable in it is TS_WAL_LEGACY_RECOVERY, two lines up. The two halves of one comment
/// named two different variables, and `tools/deploy_profile_common.sh` exported the dead one on
/// every deploy profile. Same shape as the two notes below about `TS_ENGINE_CONCURRENT_COMMIT`
/// and `TS_RAFT_APPLY_COALESCE`: the name is recorded rather than removed, so setting it still
/// finds an explanation.
pub(super) fn wal_single_barrier() -> bool {
    !wal_legacy_recovery()
}

// CONCURRENT COMMIT: run the WAL durability barrier OUTSIDE the global `shards` write lock.
// A synchronous write reserves its WAL sequence and appends its record UNDER the `shards` lock
// (preserving WAL-order == apply-order), then RELEASES the lock and awaits the durable barrier
// (`commit_barrier`). This lets concurrent same-shard writers reach the group-commit queue while
// a peer's fdatasync is in flight, so #45's fsync coalescing actually engages (fewer fsyncs than
// writes; QPS scales with concurrency). The ack is always returned strictly AFTER the covering
// barrier succeeds, so durability is never weakened.
//
// This is the `concurrent_commit` field, per engine, true everywhere but the test that measures
// what the in-lock barrier costs. `TS_ENGINE_CONCURRENT_COMMIT` used to decide it for every
// engine in the process at once; it is read by nothing now, so setting it does nothing.


// RAFT APPLY COALESCE: on the raft state-machine apply path, coalesce the per-committed-entry
// engine-WAL fdatasync across a whole committed batch (one fsync per AppendEntries batch /
// recovery replay / pipelined-propose group instead of one per entry) and anchor the served index
// off the O(1) cached WAL sequence. The raft log stays the durability + reconstruction source;
// the coalesced barrier still completes before the raft runtime advances the durable
// applied_index.
//
// This is the `raft_apply_coalesce` field, per engine, true everywhere but the test that measures
// the per-entry loop. `TS_RAFT_APPLY_COALESCE` used to decide it for every engine in the process
// at once; it is read by nothing now, so setting it does nothing.

fn env_flag_on(name: &str) -> bool {
    crate::env_flag::env_bool(name, false)
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

/// Default-ON gate read: the fix is LIVE unless explicitly disabled with
/// `=0|false|no|off`. Shipped write-path/raft fixes use this so production gets the
/// fixed behavior by default; the env var remains only as an escape hatch.
pub(crate) fn env_flag_default_on(name: &str) -> bool {
    crate::env_flag::env_bool(name, true)
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
    block_store: &BlockStore,
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
    let routing_bucket = block_routing_bucket(&compression_key, start_routing_bucket, end_routing_bucket);
    let mut mutated = false;
    if let Ok(addresses) = append_timestamped_kv_blocks(
        cache,
        block_store,
        shard_id,
        "context_compression",
        &compression_key,
        vec![FeaturePoint {
            timestamp_ms: timeline_key,
            value: context_bytes(&compression_event),
        }],
        routing_bucket,
        async_storage,
        false,
        crate::engine::state::next_block_index_for_object(
            &shard.bucket_index,
            routing_bucket,
            "context_compression",
            &compression_key,
        ),
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
    invalidate_context_record(cache, shard_id, &compression_key);
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
        .map(|expires_at| expires_at.saturating_sub(resolve_now_ms()) as i64)
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

/// How many deadlines disagree between the two expiry indexes. Zero is the invariant.
///
/// Exposed so a guard can assert it after a real workload: the two maps are kept in step by
/// `set_expiry`/`clear_expiry`, and a mutation site that bypassed them would show up here rather
/// than as keys that silently never expire.
pub(in crate::engine) fn expiry_index_disagreements(shard: &ShardState) -> usize {
    let mut disagreements = 0usize;
    for (key, expires_at) in shard.expires_at_ms.iter() {
        if !shard
            .expiry_by_deadline
            .contains_key(&(*expires_at, key.clone()))
        {
            disagreements = disagreements.saturating_add(1);
        }
    }
    for ((expires_at, key), ()) in shard.expiry_by_deadline.iter() {
        if shard.expires_at_ms.get(key) != Some(expires_at) {
            disagreements = disagreements.saturating_add(1);
        }
    }
    disagreements
}

/// Record a deadline for `key`, keeping both expiry indexes in step.
pub(in crate::engine) fn set_expiry(shard: &mut ShardState, key: String, expires_at: u64) {
    ensure_expiry_order(shard);
    if let Some(previous) = shard.expires_at_ms.insert(key.clone(), expires_at) {
        shard.expiry_by_deadline.remove(&(previous, key.clone()));
    }
    shard.expiry_by_deadline.insert((expires_at, key), ());
}

/// Drop any deadline for `key`. Returns whether there was one.
pub(in crate::engine) fn clear_expiry(shard: &mut ShardState, key: &str) -> bool {
    ensure_expiry_order(shard);
    match shard.expires_at_ms.remove(key) {
        Some(previous) => {
            shard.expiry_by_deadline.remove(&(previous, key.to_string()));
            true
        }
        None => false,
    }
}

/// Rebuild the deadline-ordered view if a load left it empty.
///
/// It carries `#[serde(skip)]`, so a shard restored from a snapshot has the key-ordered map and
/// not this one. Rebuilding on first use keeps the persisted format unchanged and costs one pass
/// per load rather than a migration.
pub(in crate::engine) fn ensure_expiry_order(shard: &mut ShardState) {
    if shard.expiry_by_deadline.is_empty() && !shard.expires_at_ms.is_empty() {
        for (key, expires_at) in shard.expires_at_ms.iter() {
            shard
                .expiry_by_deadline
                .insert((*expires_at, key.clone()), ());
        }
    }
}

/// The keys whose deadline has passed, cheapest first, up to `limit` that `keep` accepts.
///
/// Due keys are a PREFIX of the deadline-ordered view, so this stops at the first deadline in the
/// future instead of walking the keyspace. `scan_budget` still bounds the walk, because `keep`
/// can reject a long run of due keys belonging to the other class.
pub(in crate::engine) fn due_window<F>(
    shard: &ShardState,
    now: u64,
    limit: usize,
    scan_budget: usize,
    keep: F,
) -> Vec<(String, u64)>
where
    F: Fn(&str) -> bool,
{
    let mut selected = Vec::new();
    let mut walked = 0usize;
    for ((expires_at, key), ()) in shard.expiry_by_deadline.iter() {
        if *expires_at > now {
            break;
        }
        if limit > 0 && selected.len() >= limit {
            break;
        }
        if scan_budget > 0 && walked >= scan_budget {
            break;
        }
        walked = walked.saturating_add(1);
        if keep(key.as_str()) {
            selected.push((key.clone(), *expires_at));
        }
    }
    selected
}

fn remove_if_expired(shard: &mut ShardState, key: &str) -> bool {
    // Use the replay-aware clock: during WAL replay this resolves to the per-record leader
    // timestamp so lazy expiry reproduces the leader's original branch. Using the real
    // restart clock here would let a key that was live at leader-time (and thus took the
    // "exists" branch of a logged conditional write) appear expired on recovery, silently
    // dropping a durably-committed write and diverging the recovered state from the leader.
    // Nothing can have expired if nothing has an expiry. Checked before anything else because
    // this runs on every command -- 51 call sites -- and the walk below used to build four owned
    // keys, one of them with `format!`, purely to look them up and drop them again.
    if shard.expires_at_ms.is_empty() {
        return false;
    }

    let now = resolve_now_ms();
    let mut removed = false;

    let expired = |shard: &ShardState, candidate: &str, now: u64| {
        shard
            .expires_at_ms
            .get(candidate)
            .is_some_and(|expires_at| *expires_at <= now)
    };

    // The key as given. `BTreeMap<String, _>` looks up by `&str`, so this needs no owned copy.
    if expired(shard, key, now) {
        removed |= delete_record_exact(shard, key);
    }

    if key.starts_with("control_state:") {
        return removed;
    }

    // The control-state families. Built into one buffer that is rewritten per family rather than
    // a fresh `String` apiece.
    let mut candidate = String::with_capacity("control_state:".len() + 4 + key.len());
    for family in [
        ControlStateFamily::Counter,
        ControlStateFamily::Distinct,
        ControlStateFamily::Selection,
    ] {
        candidate.clear();
        candidate.push_str("control_state:");
        candidate.push_str(control_state_family_name(family));
        candidate.push(':');
        candidate.push_str(key);
        if expired(shard, &candidate, now) {
            removed |= delete_record_exact(shard, &candidate);
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
    removed |= clear_expiry(shard, key);
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
    removed |= shard.control_state_blocks.remove(key).is_some();
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
    // A RELEASED bucket holds no page entries, so the walk below finds nothing to remove and the
    // object id would stay claimed until some later reload re-derived the set. Settle it here,
    // while the model map this reads the address out of still holds the page.
    //
    // Kept out of `removed`: that flag also decides whether the object-page lookup is corrected,
    // and a released bucket's lookup entries were dropped by the release itself. There is nothing
    // there to correct, and establishing the lookup as a side effect of a delete is not this
    // change's to make.
    let settled_released =
        crate::engine::storage_bucket_internals::settle_released_bucket_object_delete(shard, key);
    let mut removed = false;
    let target_buckets = bucket_index_target_buckets_for_object_key(shard, key);
    for routing_bucket in target_buckets {
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        let mut deleted_object_ids = BTreeSet::new();
        bucket.block_index.retain(&mut shard.bucket_index.block_slab_live, |_, page| {
            if &*page.object_key == key {
                deleted_object_ids.insert(page.object_id());
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
            bucket.deleted = bucket.block_index.is_empty();
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
            bucket.meta_loaded = true;
            bucket.in_memory = !bucket.block_index.is_empty();
            update_bucket_layout(bucket);
        }
    }
    if removed {
        // Drop this object's own entries rather than rebuilding the lookup for the whole shard.
        // The rebuild cloned every page in every bucket, so a single delete allocated in
        // proportion to the store -- 409 allocations at 200 resident keys, 3,851 at 3,200 -- and
        // deleting a store cost the square of its size.
        //
        // Except when the lookup is not established yet. The ref total counts as part of that:
        // only a rebuild can set it, since a count that starts at "unknown" cannot be decremented
        // into a right answer, and the load path fills the lookup without one. The rebuild this
        // replaces was establishing the total on every delete, which hid that. So establish both
        // here on the first delete that finds them unset, and take the cheap path thereafter --
        // the same bargain the typed removal path makes.
        if shard.bucket_index.object_block_lookup.is_empty()
            || shard.bucket_index.object_component_block_refs.is_none()
        {
            shard.bucket_index.rebuild_object_block_lookup();
        } else {
            for kind in storage_model_kinds() {
                shard.bucket_index.remove_object_from_block_lookup(kind, key);
            }
        }
    }
    removed || settled_released
}

fn bucket_index_target_buckets_for_object_key(shard: &ShardState, key: &str) -> BTreeSet<u32> {
    if shard.bucket_index.object_block_lookup.is_empty() {
        return shard.bucket_index.bucket_map.keys().copied().collect();
    }
    let mut buckets = BTreeSet::new();
    for kind in storage_model_kinds() {
        if let Some(entry) = shard.bucket_index.object_block_refs(kind, key) {
            buckets.extend(entry.all_refs().map(|block_ref| block_ref.routing_bucket));
        }
    }
    buckets
}

fn mark_bucket_index_block_deleted(
    shard: &mut ShardState,
    shard_id: ShardId,
    model_id: &str,
    key: &str,
    component: Option<&str>,
) -> bool {
    mark_bucket_index_block_deleted_with(shard, shard_id, model_id, key, component, true)
}

/// The same, with a say over whether an outcome is staged for the record.
///
/// Replay INSTALLS a removal that was already recorded; staging another from inside the install
/// would record the recovery as a write of its own. Same shape as
/// `upsert_bucket_index_block_with`, and the same reason.
fn mark_bucket_index_block_deleted_with(
    shard: &mut ShardState,
    shard_id: ShardId,
    model_id: &str,
    key: &str,
    component: Option<&str>,
    stage: bool,
) -> bool {
    // Removing a member IS an outcome, and it is the one a command log states worst: replay has
    // to re-run the removal and hope the state it removes from matches. Saying "this component
    // is gone" needs no such hope. Recorded here because every typed removal comes through.
    if stage {
        block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
            kind: model_id.to_string(),
            object_key: key.to_string(),
            component: component.map(str::to_string),
            object_id: stable_block_object_id(shard_id, model_id, key, component),
            routing_bucket: block_routing_bucket(key, 0, u32::MAX),
            address: None,
            value: None,
            ttl: None,
            deleted: true,
            meta: true,
        });
    }
    let mut removed = false;
    let target_buckets = if shard.bucket_index.object_block_lookup.is_empty() {
        shard
            .bucket_index
            .bucket_map
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
    } else {
        shard
            .bucket_index
            .block_refs_for(model_id, key, component)
            .map(|block_refs| {
                block_refs
                    .iter()
                    .map(|block_ref| block_ref.routing_bucket)
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
        bucket.block_index.retain(&mut shard.bucket_index.block_slab_live, |_, page| {
            let matches = page.model_id.as_ref() == model_id
                && &*page.object_key == key
                && page.component.as_deref() == component;
            if matches {
                deleted_object_ids.insert(page.object_id());
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
            bucket.deleted = bucket.block_index.is_empty();
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
            bucket.meta_loaded = true;
            bucket.in_memory = !bucket.block_index.is_empty();
            update_bucket_layout(bucket);
        }
    }
    if removed {
        // Removes exactly the entry it deleted, instead of rebuilding the whole lookup.
        //
        // `rebuild_object_block_lookup` clears `object_page_lookup` and `object_component_lookup`
        // and re-inserts one entry per page in the shard -- 58 696 of them on a 250 MB store --
        // and this ran once per deleted page. A purge deleting five fields paid it five times.
        // That is why deleting an identical, freshly created memory cost 41.7 ms against a 20 MB
        // store and 385.7 ms against a 249 MB one, for provably the same closure: same four ids,
        // same 96 records scanned, same five fields rewritten. Identical work, nine times the
        // time, all of it spent rebuilding a lookup to the same shape it already had minus one
        // entry.
        //
        // This is the exact inverse of the `insert_object_block_lookup` the page went in through,
        // keyed on the same (model_id, object_key, component) -- which is how the upsert path
        // has always maintained the lookup. The whole-object deleter above still rebuilds; it
        // drops every component of a key at once, so the entry-at-a-time inverse does not apply
        // to it unchanged.
        shard
            .bucket_index
            .remove_object_block_lookup_entry(model_id, key, component);
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

/// Absorb one data model's block addresses into the live set, charging every address visited.
///
/// The set this function returns is tiny -- one entry per slab -- and the walk that fills it is
/// the size of the CORPUS. That difference is invisible in the return value, so it is charged
/// here, by reference: a data model added to `collect_live_block_slab_ids` has to go through this
/// to reach `ids`, and therefore cannot be walked without being counted.
fn absorb_live_block_slab_ids(
    ids: &mut BTreeSet<u64>,
    addresses_visited: &mut u64,
    addresses: impl Iterator<Item = u64>,
) {
    for block_slab_id in addresses {
        *addresses_visited += 1;
        ids.insert(block_slab_id);
    }
}

fn collect_live_block_slab_ids(shard: &ShardState) -> BTreeSet<u64> {
    let mut ids = BTreeSet::new();
    let mut visited = 0u64;
    absorb_live_block_slab_ids(
        &mut ids,
        &mut visited,
        shard.strings.values().map(|address| address.block_slab_id),
    );
    for fields in shard.hashes.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            fields.values().map(|address| address.block_slab_id),
        );
    }
    for members in shard.sets.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            members.values().map(|address| address.block_slab_id),
        );
    }
    for elements in shard.lists.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            elements.values().map(|address| address.block_slab_id),
        );
    }
    for members in shard.zsets.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            members.values().map(|(_, address)| address.block_slab_id),
        );
    }
    for series in shard.features.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    absorb_live_block_slab_ids(
        &mut ids,
        &mut visited,
        shard
            .context_nodes
            .values()
            .map(|address| address.block_slab_id),
    );
    for series in shard.context_events.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    for series in shard.context_indexes.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    for series in shard.context_audits.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    for series in shard.context_entities.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    for series in shard.context_children.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    for series in shard.context_summaries.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    for series in shard.context_compressions.values() {
        absorb_live_block_slab_ids(
            &mut ids,
            &mut visited,
            series.values().map(|address| address.block_slab_id),
        );
    }
    // control_state_pages is the page-backed control-state model and MUST be in the
    // GC live set: it feeds both the reclaim live-slab set and the page-gc dependency plan.
    // Omitting it let a slab holding only a control-state page be reclaimed while the index
    // still referenced it -> DataLoss on the next read. keeps any model's live pages
    // counted in the zone's used_bytes so the zone is never destroyed while referenced. The
    // sibling collect_model_live_block_entries already includes it -- the two lists had drifted.
    absorb_live_block_slab_ids(
        &mut ids,
        &mut visited,
        shard
            .control_state_blocks
            .values()
            .map(|address| address.block_slab_id),
    );
    #[cfg(test)]
    crate::snapshot_probe::note_live_slab_scan(visited);
    let _ = visited;
    ids
}

fn append_value(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    bytes: &[u8],
    object_id: Option<u64>,
    routing_bucket: Option<u32>,
    async_storage: bool,
) -> Result<BlockAddress, BlockStoreError> {
    // Both arms, and every command that stores a value, reach a slab through here. The payload's
    // own copies re-tag themselves inside -- the encode as `PageBytes`, the carried copy as
    // `CarriedPage` -- so what is left under this class is the append machinery and not the bytes.
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::SlabAppend, || {
        append_value_inner(
            cache,
            block_store,
            shard_id,
            bytes,
            object_id,
            routing_bucket,
            async_storage,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn append_value_inner(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    bytes: &[u8],
    object_id: Option<u64>,
    routing_bucket: Option<u32>,
    async_storage: bool,
) -> Result<BlockAddress, BlockStoreError> {
    if !async_storage {
        // Carry the page in this write's record, the same as the asynchronous arm below.
        //
        // The record is allowed to state its results and drop the operation only when the blocks
        // those results name survive a crash. A carried block does -- it IS the record. A
        // synchronous write's block used to be assumed durable instead, but the single barrier
        // acks on the WAL fsync and defers the block fsync (`defer_data_sync` in the block
        // store's append), so at that moment the block store holds it in buffers and nowhere
        // else. Wiping everything but the log then lost every acked write, which is what the
        // recovery suite has been reporting.
        //
        // Carrying it costs the bytes twice for as long as the record lives, and no longer: the
        // storage manager's reclaim stage moves carried pages into the block store and drops the
        // registration that pins the log floor.
        if let Some(object_id) = object_id {
            block_in_wal::stage(object_id, bytes);
        }
        return block_store.append_with_block_metadata(bytes, object_id, routing_bucket);
    }
    let address = BlockAddress::from_parts(HOT_BLOCK_SLAB_ID, HOT_BLOCK_OFFSET.fetch_add(1, Ordering::Relaxed), bytes.len() as u64, None, object_id, routing_bucket, object_id);
    // Put the page aside for this write's record. It is often derived state rather than the
    // command's own bytes, so the record has to carry it for a read to serve it back.
    if let Some(object_id) = object_id {
        block_in_wal::stage(object_id, bytes);
    }
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::PageBytes, || {
        let bytes = bytes.to_vec();
        cache.put_memory_only(
            CacheKey::page_with_slot(
                shard_id,
                address.block_slab_id,
                address.offset,
                address.length,
                address.routing_bucket()),
            bytes,
        );
    });
    Ok(address)
}

fn persist_control_state_block(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let Some(series) = shard.control_state.get(key) else {
        shard.control_state_blocks.remove(key);
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
    let object_id = stable_block_object_id(shard_id, "control_state", key, None);
    let routing_bucket = block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    if let Ok(address) = append_value(
        cache,
        block_store,
        shard_id,
        &bytes,
        Some(object_id),
        Some(routing_bucket),
        async_storage,
    ) {
        upsert_bucket_index_block(shard, shard_id, "control_state", key, None, address.clone(), true);
        shard.control_state_blocks.insert(key.to_string(), address);
        true
    } else {
        false
    }
}

fn invalidate_cache_key(cache: &MultiLayerCache, key: CacheKey, memory_only: bool) {
    // The largest class by allocation count on a value write, and it took a measurement to find
    // out: invalidating one key costs more calls than the whole rest of the write together. The
    // key itself is built by the caller and is not in here.
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::CacheInvalidation, || {
        invalidate_cache_key_inner(cache, key, memory_only)
    })
}

fn invalidate_cache_key_inner(cache: &MultiLayerCache, key: CacheKey, memory_only: bool) {
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
    let bucket_index_exists = if shard.bucket_index.object_block_lookup.is_empty() {
        shard.bucket_index.bucket_map.values().any(|bucket| {
            bucket.block_index
                .values()
                .any(|page| &*page.object_key == key && !page.deleted)
        })
    } else {
        storage_model_kinds().iter().any(|kind| {
            shard
                .bucket_index
                .object_block_refs(kind, key)
                .map(|block_refs| {
                    block_refs.all_refs().any(|block_ref| {
                        shard
                            .bucket_index
                            .bucket_map
                            .get(&block_ref.routing_bucket)
                            .and_then(|bucket| bucket.block_index.get(&block_ref.block_ref_key))
                            .map(|page| {
                                !page.deleted && page.model_id.as_ref() == *kind && &*page.object_key == key
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
        || shard.control_state_blocks.contains_key(key)
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

/// The invalidation a page-backed context object actually needs.
///
/// `invalidate_record_all` also sweeps the `hash`, `set` and `feature` record namespaces, and each
/// of those three is `MultiLayerCache::invalidate_record`, which walks EVERY key in all three
/// cache tiers and filters. The cost therefore grows with the cache, and a context object is never
/// cached in any of those namespaces -- so all three walked the whole cache, found nothing, and did
/// it again on the next write.
///
/// That is what made message ingest degrade with the corpus rather than stay flat. Measured on the
/// real four-command message, growing one store:
///
///     corpus  1,024      4.6 ms/message  ->  7.2 ms
///     corpus 10,240     29.1 ms/message  ->  7.9 ms      and no longer rising
///
/// `a_context_write_leaves_the_record_namespaces_empty` holds the premise: if a context object ever
/// does get cached under one of those namespaces, that test fails and this narrowing is no longer
/// safe.
fn invalidate_context_record(cache: &MultiLayerCache, shard_id: ShardId, key: &str) {
    let _ = cache.invalidate(&CacheKey::string(shard_id, key));
}

/// What one `invalidate_record_all` costs, counted where the cost LANDS.
///
/// The price of a sweep is not a constant. `MultiLayerCache::invalidate_record` chains the key
/// sets of all three tiers -- memory, pmem, disk index -- and filters, so one call steps over the
/// WHOLE CACHE, and `invalidate_record_all` makes two of them. A fixture that writes a corpus and
/// never reads it back leaves those tiers nearly empty, which measures the sweep at its floor
/// rather than at what a serving store pays.
///
/// So the quantity that matters is not the call count -- that is one per key and has never been
/// in doubt -- but ENTRIES WALKED, and that is a property of the cache at the moment of the call.
/// It is read from the three tier lengths, which are exactly the key sets the walk chains.
///
/// HANDED IN rather than read off a free static: a new call site has to name where its
/// sweeps are counted before it compiles.
///
/// `entries_walked` costs three uncontended tier-length reads per sweep, so it sits behind
/// `armed` and is off unless a probe turns it on. `arming_the_sweep_counter_does_not_move_the_hold`
/// is the control for that, and it is what makes the timings below readable next to the counts.
#[derive(Debug)]
pub(crate) struct CacheSweepCounts {
    armed: std::sync::atomic::AtomicBool,
    /// `invalidate_record_all` calls.
    pub calls: std::sync::atomic::AtomicU64,
    /// `MultiLayerCache::invalidate_record` calls -- the walks, two per call.
    pub sweeps: std::sync::atomic::AtomicU64,
    /// Cache entries those walks stepped over, summed over the sweeps. Zero unless armed.
    pub entries_walked: std::sync::atomic::AtomicU64,
    /// Named-key invalidations: one `CacheKey`, no walk. What a sweep is narrowed TO.
    pub named: std::sync::atomic::AtomicU64,
    /// `invalidate_records_all_batched` calls -- ONE per round, not one per dropped key.
    pub batched_calls: std::sync::atomic::AtomicU64,
    /// Dropped keys those calls were handed, counted INSIDE the primitive.
    pub keys_batched: std::sync::atomic::AtomicU64,
    /// `MultiLayerCache::entries_for_shard` listings -- the batched pass's single walk.
    pub listings: std::sync::atomic::AtomicU64,
    /// Cache entries those listings stepped over, summed. Zero unless armed, and read from the
    /// SAME three tier lengths as `entries_walked`, so the two are commensurable.
    pub entries_listed: std::sync::atomic::AtomicU64,
    /// Entries those listings RETURNED for the shard. This is the upper bound on the filesystem
    /// `metadata()` calls the listing makes: `entries_for_shard` falls through to
    /// `disk_path(&key).metadata()` for every returned entry the disk index does not hold, and
    /// for no others. Always counted -- it is a `Vec::len()`, not a probe.
    pub entries_returned: std::sync::atomic::AtomicU64,
}

impl CacheSweepCounts {
    const fn zeroed() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            calls: std::sync::atomic::AtomicU64::new(0),
            sweeps: std::sync::atomic::AtomicU64::new(0),
            entries_walked: std::sync::atomic::AtomicU64::new(0),
            named: std::sync::atomic::AtomicU64::new(0),
            batched_calls: std::sync::atomic::AtomicU64::new(0),
            keys_batched: std::sync::atomic::AtomicU64::new(0),
            listings: std::sync::atomic::AtomicU64::new(0),
            entries_listed: std::sync::atomic::AtomicU64::new(0),
            entries_returned: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Process-wide, so a reader resets immediately before the round it measures and the suite it
    /// runs in is single-threaded.
    pub(crate) fn reset(&self) {
        for cell in [
            &self.calls,
            &self.sweeps,
            &self.entries_walked,
            &self.named,
            &self.batched_calls,
            &self.keys_batched,
            &self.listings,
            &self.entries_listed,
            &self.entries_returned,
        ] {
            cell.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub(crate) fn set_armed(&self, armed: bool) {
        self.armed
            .store(armed, std::sync::atomic::Ordering::Relaxed);
    }

    /// `(calls, sweeps, entries_walked, named)`.
    pub(crate) fn read(&self) -> (u64, u64, u64, u64) {
        let load = |cell: &std::sync::atomic::AtomicU64| {
            cell.load(std::sync::atomic::Ordering::Relaxed)
        };
        (
            load(&self.calls),
            load(&self.sweeps),
            load(&self.entries_walked),
            load(&self.named),
        )
    }

    fn note_call(&self) {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// `(batched_calls, keys_batched, listings, entries_listed, entries_returned)`.
    pub(crate) fn read_batched(&self) -> (u64, u64, u64, u64, u64) {
        let load = |cell: &std::sync::atomic::AtomicU64| {
            cell.load(std::sync::atomic::Ordering::Relaxed)
        };
        (
            load(&self.batched_calls),
            load(&self.keys_batched),
            load(&self.listings),
            load(&self.entries_listed),
            load(&self.entries_returned),
        )
    }

    fn note_named(&self) {
        self.named
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// One batched pass, and the number of dropped keys it was handed.
    ///
    /// `keys` is counted HERE, inside the primitive, so it is independent of the dropped-key
    /// count `apply_storage_eviction` returns to its caller. The difference between the two is
    /// the round's residual.
    fn note_batched_call(&self, keys: u64) {
        self.batched_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.keys_batched
            .fetch_add(keys, std::sync::atomic::Ordering::Relaxed);
    }

    /// Counted immediately BEFORE the listing it describes, from the same three tier lengths
    /// `note_sweep` reads -- so a listing and a sweep are priced in the same unit and the ratio
    /// between them means something.
    fn note_listing(&self, cache: &MultiLayerCache) {
        self.listings
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.armed.load(std::sync::atomic::Ordering::Relaxed) {
            let listed = cache
                .item_count_for_tier(matrixcache::CacheTier::Memory)
                .saturating_add(cache.item_count_for_tier(matrixcache::CacheTier::Pmem))
                .saturating_add(cache.item_count_for_tier(matrixcache::CacheTier::Ssd));
            self.entries_listed
                .fetch_add(listed as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn note_listed_entries(&self, returned: u64) {
        self.entries_returned
            .fetch_add(returned, std::sync::atomic::Ordering::Relaxed);
    }

    /// Counted immediately BEFORE the walk it describes, so the tier lengths it reads are the
    /// ones that walk will chain -- not the ones left after it has removed entries.
    fn note_sweep(&self, cache: &MultiLayerCache) {
        self.sweeps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.armed.load(std::sync::atomic::Ordering::Relaxed) {
            let walked = cache
                .item_count_for_tier(matrixcache::CacheTier::Memory)
                .saturating_add(cache.item_count_for_tier(matrixcache::CacheTier::Pmem))
                .saturating_add(cache.item_count_for_tier(matrixcache::CacheTier::Ssd));
            self.entries_walked
                .fetch_add(walked as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

pub(crate) static CACHE_SWEEP_COUNTS: CacheSweepCounts = CacheSweepCounts::zeroed();

/// The record namespaces a dropped key has to SWEEP for, because neither has a single cacheable
/// key to name: a hash caches one entry per FIELD and a feature one per query window.
///
/// THE AUTHORITY, and both arms read it. The per-key primitive below and the batched pass both
/// iterate this list, so a namespace added here is covered by both and cannot be covered by only
/// one. The equivalence test enumerates from here too, rather than from a list written out beside
/// it -- a hand-written copy goes stale and nothing fails.
pub(crate) const SWEPT_RECORD_NAMESPACES: [&str; 2] = ["hash", "feature"];

/// The record entries a dropped key can NAME instead of sweeping for, one `CacheKey` each.
///
/// `string` has the fixed selector "value", and a set has exactly ONE cacheable entry because
/// `CacheKey::set_members` fixes its selector at "members" -- so naming them is equivalent to
/// sweeping for them, and it is O(1) instead of O(cache).
///
/// THE AUTHORITY, again shared by both arms, for the same reason.
fn named_record_keys(shard_id: ShardId, key: &str) -> [CacheKey; 2] {
    [
        CacheKey::string(shard_id, key),
        CacheKey::set_members(shard_id, key),
    ]
}

fn invalidate_record_all(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    key: &str,
    counts: &CacheSweepCounts,
) {
    counts.note_call();
    for named in named_record_keys(shard_id, key) {
        counts.note_named();
        let _ = cache.invalidate(&named);
    }
    for namespace in SWEPT_RECORD_NAMESPACES {
        counts.note_sweep(cache);
        let _ = cache.invalidate_record(shard_id, namespace, key);
    }
}

/// ONE PASS OVER THE CACHE FOR A WHOLE ROUND OF DROPPED KEYS, instead of two per key.
///
/// BOTH LOGGED-DELETION ROUNDS USE THIS ONE BODY. `delete_drop` in `storage_lifecycle_methods.rs`
/// (#1911) and the expiry sweep in `recovery_sweep_compact.rs` had the same per-key shape under the
/// same `shards` write guard, and a pass that lived in only one of them would have left the other
/// copy keeping the cost -- the way the anchor's `stats()` fallback did until one of the two was
/// guarded and the other was not. The expiry sweep's own figures are in
/// `what_an_expiry_rounds_cache_sweep_walks_cold_and_warm` and
/// `one_expiry_pass_walks_the_cache_once_instead_of_twice_per_expired_key`, measured on that path
/// rather than inherited from this one: 704,548 cache-entry visits for a 250-key round on a warm
/// 500-object store and 44,134,298 for a 2,000-key round on a warm 4,000-object one, against 1,548
/// and 12,048 for the one pass.
///
/// WHAT IT REPLACES. `invalidate_record_all` above, called once per dropped key, makes two
/// `MultiLayerCache::invalidate_record` calls, and each of those chains the key sets of all three
/// cache tiers and filters:
///
///     inner.memory.keys().chain(inner.pmem.keys()).chain(inner.disk_index.keys())
///         .filter(|key| key.shard_id == shard_id && key.namespace == namespace && ...)
///
/// So N dropped keys walk the cache 2N times. #1907 measured that at 3,999 entries per dropped
/// key on a warm 4,000-object store -- 15,996,000 entry visits for one round, inside the `shards`
/// write guard that serving reads queue behind.
///
/// WHAT IT DOES INSTEAD. The swept namespaces are the same for every key in the round, so the
/// walk does not have to be repeated: one listing of the shard's cache, one membership test per
/// entry against the round's dropped-key set, and one `invalidate_batch` for everything that
/// matched plus the named keys. The cache is stepped over ONCE per round rather than twice per
/// key, and the write lock inside the cache is taken once rather than 4N times.
///
/// EXACTLY THE SAME SET, arm for arm. `entries_for_shard` chains the SAME three tier key sets as
/// `invalidate_record` and filters them by shard alone, so it is a superset of what every one of
/// the round's per-key sweeps could have matched; the predicate here re-applies the other two
/// halves of that filter (`namespace` and `record_key`) that `invalidate_record` applies inline.
/// `CacheEntryInfo` carries `shard_id`, `namespace`, `record_key` and `selector`, which is every
/// field of a `CacheKey`, so the key each matched entry is rebuilt from is the key that was in
/// the tier. Both arms take their namespaces from `SWEPT_RECORD_NAMESPACES` and their named keys
/// from `named_record_keys`, so neither list can drift from the other. And a key the round did
/// NOT drop is not in the set, so its entries are left cached -- which is the same thing N
/// per-key sweeps do, and the direction that would be visible if this were wrong.
///
/// WHY IT STILL RUNS INSIDE THE GUARD. #1907 priced deferring it and refused: `cached_response`
/// is cache-first and a `string` record key carries no generation, sequence or version stamp, so
/// a reader in the window is answered with a value the shard has already deleted -- measured at 8
/// of 533 reads. This change moves no invalidation out of the `shards` write guard; it only stops
/// repeating the walk. `a_key_the_shard_has_dropped_is_never_still_answered_out_of_the_cache` is
/// what holds that.
///
/// WHAT IT COSTS INSTEAD. `entries_for_shard` falls through to a filesystem `metadata()` for
/// every entry it returns that the disk index does not hold, so the one pass carries C syscalls.
/// `what_one_listing_of_the_shards_cache_costs_in_syscalls` measures C from outside the process
/// and `one_batched_pass_walks_the_cache_once_instead_of_twice_per_dropped_key` puts it next to
/// the entry visits it removes.
/// GENERIC OVER THE KEY TYPE, and only because its two call sites hold their round's keys in
/// different containers: `delete_drop` collects `Arc<str>` object keys off the live block entries,
/// and the expiry sweep collects `String` keys out of `due_window`. Converting either one to suit
/// the other would allocate a second copy of every key in the round for no reason. `AsRef<str>` is
/// all this body ever asks of them -- it reads each key once, as a `&str`, to build the
/// dropped-key set and the named cache keys -- so both callers hand it what they already have.
fn invalidate_records_all_batched<K: AsRef<str>>(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    keys: &[K],
    counts: &CacheSweepCounts,
) -> usize {
    if keys.is_empty() {
        return 0;
    }
    counts.note_batched_call(keys.len() as u64);
    let dropped = keys
        .iter()
        .map(|key| key.as_ref())
        .collect::<std::collections::BTreeSet<&str>>();
    let mut batch = Vec::with_capacity(keys.len().saturating_mul(2));
    for key in keys {
        for named in named_record_keys(shard_id, key.as_ref()) {
            counts.note_named();
            batch.push(named);
        }
    }
    counts.note_listing(cache);
    let listed = cache.entries_for_shard(shard_id);
    counts.note_listed_entries(listed.len() as u64);
    for entry in listed {
        if SWEPT_RECORD_NAMESPACES.contains(&entry.namespace.as_str())
            && dropped.contains(entry.record_key.as_str())
        {
            batch.push(CacheKey {
                shard_id,
                record_key: entry.record_key,
                namespace: std::borrow::Cow::Owned(entry.namespace),
                selector: entry.selector,
            });
        }
    }
    cache.invalidate_batch(&batch).unwrap_or(0)
}

fn read_block_bytes(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    address: &BlockAddress,
) -> Option<Vec<u8>> {
    let cache_key = CacheKey::page_with_slot(
        shard_id,
        address.block_slab_id,
        address.offset,
        address.length,
        address.routing_bucket());
    if let Ok(Some(bytes)) = cache.get(&cache_key) {
        return Some(bytes);
    }
    // Past the cache, so this call goes to storage. Counted here rather than at each of the
    // three fallbacks below because every one of them is a trip to the store and they must not
    // drift apart in the tally; a cache HIT returns above and is not I/O.
    shard_write_guard::note_block_read();
    // Log-backed hot page (synthetic address, no block-store file): a cache miss here would read
    // as MISSING for an acked async write. If it was spilled to a real slab on eviction, resolve
    // the redirect and read the durable copy. On a genuine miss (never spilled, or spill failed)
    // this falls through to the normal read below, which returns None -- the WAL still holds the
    // value and a reload replays it.
    if crate::wal_record::is_wal_resident(address.block_slab_id) {
        if let Some(real_address) = hot_page_spill::lookup_spilled(shard_id, address.offset) {
            if let Ok(bytes) = block_store.read(&real_address) {
                let _ = cache.put(cache_key, bytes.clone());
                return Some(bytes);
            }
        }
        // Nothing spilled, so the value exists only in its WAL record -- which is where it has
        // been all along. Read it back by the log id the write registered. Tried after the
        // spill redirect because a spilled copy is a direct block-store read, while this one
        // parses a log record.
        if let Some(bytes) = address
            .object_id()
            .and_then(|object_id| block_in_wal::read_block(block_store, shard_id, object_id))
        {
            let _ = cache.put(cache_key, bytes.clone());
            return Some(bytes);
        }
    }
    if let Ok(bytes) = block_store.read(address) {
        let _ = cache.put(cache_key, bytes.clone());
        return Some(bytes);
    }
    // The block store could not answer, so try the record that carries this page.
    //
    // This is the same fallback the synthetic-address branch above performs, which until now was
    // the ONLY way to reach it: the branch is entered on `is_wal_resident(address.block_slab_id)`.
    // A synchronous write stores the real address its block-store append returned, so a read for
    // one never entered that branch, and the copy carried in its record was registered, kept
    // addressable, and never consulted.
    //
    // That is exactly the case the single barrier creates. It acks on the log fsync and defers
    // the block fsync, so a crash can lose a block the index already names. Recovery then rebuilt
    // the index, resolved a real address into a block that was never written, and answered None
    // for a durably acknowledged write -- with the value sitting in the log the whole time.
    //
    // Ordered after the block-store read, not before it: the durable copy is the common case and
    // a direct read, while this one resolves a log id and parses a record.
    if let Some(bytes) = address
        .object_id()
        .and_then(|object_id| block_in_wal::read_block(block_store, shard_id, object_id))
    {
        let _ = cache.put(cache_key, bytes.clone());
        return Some(bytes);
    }
    None
}

/// The page's bytes, shared rather than copied.
///
/// `read_block_bytes` hands back a `Vec<u8>` the cache built by copying the `Arc<[u8]>` it already
/// holds. A caller that parses the bytes and drops them pays that memcpy and that allocation for
/// nothing -- once per retrieval candidate on the node fetch.
///
/// Identical to `read_block_bytes` in every other way: same key, same lookup, same spill and log
/// fallbacks, same promotion. It differs only in not owning the result. Callers that keep or mutate
/// the bytes should keep using `read_block_bytes`.
fn read_block_shared(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    address: &BlockAddress,
) -> Option<std::sync::Arc<[u8]>> {
    let cache_key = CacheKey::page_with_slot(
        shard_id,
        address.block_slab_id,
        address.offset,
        address.length,
        address.routing_bucket());
    if let Ok(Some(bytes)) = cache.get_shared(&cache_key) {
        return Some(bytes);
    }
    // Every path below writes to the cache and hands back what it wrote, so going through
    // `read_block_bytes` keeps the spill redirect, the in-log read and the block-store read in one
    // place rather than duplicating three fallbacks that must not drift apart.
    read_block_bytes(cache, block_store, shard_id, address).map(std::sync::Arc::from)
}

fn read_block_bytes_cold(block_store: &BlockStore, address: &BlockAddress) -> Option<Vec<u8>> {
    block_store.read(address).ok()
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
        let (bucket_object_count, bucket_block_ref_count, bucket_dirty_object_count) =
            if !shard.bucket_index.object_block_lookup.is_empty() {
                (
                    // The map is keyed by OBJECT now, so its length is the object count directly.
                    // It used to be the length of a second map kept alongside this one, which is
                    // the question that stopped that map from simply being deleted.
                    shard.bucket_index.object_block_lookup.len(),
                    // Maintained incrementally; the walk is the fallback for an index that has
                    // been deserialized but not yet rebuilt. This runs on the heartbeat timer
                    // under the shard read lock, so walking here shows up as write contention.
                    shard.bucket_index.object_component_block_refs.unwrap_or_else(|| {
                        shard
                            .bucket_index
                            .object_block_lookup
                            .values()
                            .map(crate::engine::state::ObjectBlockRefs::total_refs)
                            .sum::<usize>()
                    }),
                    shard.dirty_objects.len(),
                )
            } else {
                let live_blocks = shard
                    .bucket_index
                    .bucket_map
                    .values()
                    .flat_map(|bucket| bucket.block_index.values())
                    .filter(|page| !page.deleted)
                    .collect::<Vec<_>>();
                let bucket_object_count = live_blocks
                    .iter()
                    .map(|page| {
                        (
                            page.model_id.as_ref(),
                            page.object_key.as_ref(),
                            (page.model_id.as_ref() == "hash")
                                .then(|| page.component.as_deref())
                                .flatten(),
                        )
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                let bucket_dirty_object_count = live_blocks
                    .iter()
                    .filter(|page| page.dirty || shard.dirty_objects.contains(page.object_key.as_ref()))
                    .map(|page| {
                        (
                            page.model_id.as_ref(),
                            page.object_key.as_ref(),
                            (page.model_id.as_ref() == "hash")
                                .then(|| page.component.as_deref())
                                .flatten(),
                        )
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                (bucket_object_count, live_blocks.len(), bucket_dirty_object_count)
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
        let secondary_block_ref_count = shard.strings.len()
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
        let dirty_bucket_count = if !shard.bucket_index.object_block_lookup.is_empty() {
            let mut dirty_buckets = shard
                .bucket_index
                .bucket_map
                .iter()
                .filter_map(|(bucket_id, bucket)| bucket.dirty.then_some(*bucket_id))
                .collect::<BTreeSet<_>>();
            // The loop below asks, per dirty object, which buckets hold its pages -- building a
            // composite lookup key each time. `dirty_objects` grows with the ingest, and this
            // runs on the heartbeat timer under the shard read lock, so it was the shard-sized
            // work that made writes cost more as the store grew: profiling a running ingest
            // showed `bucket_index_target_buckets_for_object_key` and `push_lookup_part` rising
            // together with `RwLock::write_contended`.
            //
            // The answer is a set of bucket ids, so it cannot exceed the number of buckets. Once
            // every bucket is already in it the loop cannot change the result, and during ingest
            // that is the normal case -- `mark_async_dirty_object` marks an object's routing
            // bucket dirty as it records the object, so the collect above already has them.
            // Stopping there is exact, not an approximation.
            let bucket_total = shard.bucket_index.bucket_map.len();
            if dirty_buckets.len() < bucket_total {
                for object_key in shard.dirty_objects.iter() {
                    dirty_buckets
                        .extend(bucket_index_target_buckets_for_object_key(shard, object_key));
                    if dirty_buckets.len() >= bucket_total {
                        break;
                    }
                }
            }
            dirty_buckets.len()
        } else {
            shard
                .bucket_index
                .bucket_map
                .values()
                .filter(|bucket| {
                    bucket.dirty
                        || bucket.block_index.values().any(|page| {
                            page.dirty || shard.dirty_objects.contains(page.object_key.as_ref())
                        })
                })
                .count()
        };
        return ObjectManagerStats {
            object_count,
            block_ref_count: bucket_block_ref_count.max(secondary_block_ref_count),
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
    let block_ref_count = shard.strings.len()
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
    // The buckets the dirty index already holds these objects under. This was a hash per dirty
    // key to recompute `block_routing_bucket(key, start, end)`, which is the same function, with
    // the same arguments, that recorded them.
    dirty_buckets.extend(shard.dirty_objects.bucket_ids());
    ObjectManagerStats {
        object_count,
        block_ref_count,
        dirty_object_count: shard.dirty_objects.len(),
        dirty_bucket_count: dirty_buckets.len(),
        routing_bucket_count,
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod flag_default_doc_tests {
    use std::path::Path;

    /// A flag's doc comment must not claim the opposite default from its code.
    ///
    /// Seven of them did: they called `env_flag_default_on` -- true unless the variable is
    /// explicitly 0/false/no/off -- while their comment said "Default OFF", usually with a
    /// reassuring "byte-identical / exactly as before" after it. A reader asking the question
    /// that matters ("is this path actually live?") got the wrong answer, and nothing failed,
    /// because a comment cannot fail. This reads the sources so it can.
    #[test]
    fn flag_docs_state_the_default_the_code_actually_has() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut wrong: Vec<String> = Vec::new();
        let mut checked = 0usize;

        let mut files: Vec<std::path::PathBuf> = Vec::new();
        collect_rs(&root, &mut files);
        for file in files {
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            let lines: Vec<&str> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                let trimmed = line.trim_start();
                if !trimmed.starts_with("fn ") || !trimmed.ends_with("() -> bool {") {
                    continue;
                }
                // body: up to the closing brace at the same indent
                let mut body = String::new();
                for next in lines.iter().skip(index + 1) {
                    if next.trim_start() == "}" && next.len() - next.trim_start().len()
                        == line.len() - line.trim_start().len()
                    {
                        break;
                    }
                    body.push_str(next);
                    body.push('\n');
                }
                let actual_on = if body.contains("env_flag_default_on") {
                    true
                } else if body.contains("env_flag_on") {
                    false
                } else {
                    continue;
                };

                // doc block immediately above
                let mut doc = String::new();
                for back in (0..index).rev() {
                    let candidate = lines[back].trim_start();
                    if candidate.starts_with("///") {
                        doc.insert_str(0, &format!("{candidate}\n"));
                    } else if candidate.starts_with("#[") {
                        continue;
                    } else {
                        break;
                    }
                }
                if doc.is_empty() {
                    continue;
                }
                checked += 1;
                let low = doc.to_ascii_lowercase();
                let says_off = low.contains("default off") || low.contains("defaults off");
                let says_on = low.contains("default on") || low.contains("defaults on");
                let name = trimmed
                    .trim_start_matches("fn ")
                    .split('(')
                    .next()
                    .unwrap_or("?");
                if actual_on && says_off && !says_on {
                    wrong.push(format!(
                        "{}::{name} documents \"default off\" but calls env_flag_default_on",
                        file.file_name().unwrap_or_default().to_string_lossy()
                    ));
                }
                if !actual_on && says_on && !says_off {
                    wrong.push(format!(
                        "{}::{name} documents \"default on\" but calls env_flag_on",
                        file.file_name().unwrap_or_default().to_string_lossy()
                    ));
                }
            }
        }

        assert!(checked > 0, "no documented flag functions found; the scan is broken");
        assert!(
            wrong.is_empty(),
            "flag docs contradict their code ({} of {checked} checked):\n  {}",
            wrong.len(),
            wrong.join("\n  ")
        );
    }

    fn collect_rs(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
}

#[cfg(test)]
mod key_state_shape_tests {
    use super::*;

    /// The property the capture side now relies on: a field LEFT OUT and a field written as
    /// an explicit null mean the same thing to the apply side. If that ever stops being true,
    /// omitting the nulls silently stops tombstoning keys on reload, and evicted membership
    /// comes back from the pages.
    #[test]
    fn an_omitted_key_state_field_delete_markers_exactly_like_an_explicit_null() {
        let mut with_null: HashMap<String, u64> = HashMap::new();
        with_null.insert("k".to_string(), 7);
        apply_key_state_field(&mut with_null, "k", Some(&serde_json::Value::Null));

        let mut omitted: HashMap<String, u64> = HashMap::new();
        omitted.insert("k".to_string(), 7);
        apply_key_state_field(&mut omitted, "k", None);

        assert_eq!(with_null.get("k"), None, "an explicit null must remove the entry");
        assert_eq!(omitted.get("k"), None, "an omitted field must remove it too");
        assert_eq!(with_null, omitted);

        // Not vacuous: a PRESENT value must still be inserted, so this cannot pass on an
        // apply that removes everything.
        let mut present: HashMap<String, u64> = HashMap::new();
        apply_key_state_field(&mut present, "k", Some(&serde_json::json!(9)));
        assert_eq!(present.get("k"), Some(&9));
    }

    #[test]
    fn a_key_in_no_map_captures_only_its_key() {
        // The common case: an ordinary write touches a key that is in none of the thirteen
        // maps. Previously that produced thirteen nulls; now it produces nothing but the key.
        let shard = ShardState::default();
        let blobs = capture_key_states(&shard, &["m:0".to_string()]);
        assert_eq!(blobs.len(), 1);
        let object = blobs[0].as_object().expect("a blob is an object");
        assert_eq!(object.get("key").and_then(|v| v.as_str()), Some("m:0"));
        assert_eq!(
            object.len(),
            1,
            "a key in no map should carry only its own name, got {object:?}"
        );
    }
}
