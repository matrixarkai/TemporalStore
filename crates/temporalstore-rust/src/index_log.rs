// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::block_store::BlockAddress;
use crate::types::ShardId;

#[derive(Debug, Error)]
pub enum IndexLogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A committed, newline-terminated served-index-log record failed its per-record
    /// integrity envelope (framing length / SHA-256 digest) or violated delta
    /// sequence-continuity. Surfaced as data loss so the load aborts instead of silently
    /// skipping a delta (an eviction/removal recorded ONLY in the delta -- not the WAL --
    /// would otherwise be lost, resurrecting the removed point or dangling its page ref).
    #[error("index-log record integrity error: {0}")]
    Corruption(String),
}

impl From<crate::log_framing::FramingError> for IndexLogError {
    fn from(err: crate::log_framing::FramingError) -> Self {
        IndexLogError::Corruption(err.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexLogRecord {
    pub shard_id: ShardId,
    pub sequence: u64,
    // Default so a delta-record line (which carries `items`/`meta` but no `index`) still
    // parses as an IndexLogRecord for the sequence-tail scan (`last_sequence_at`), which
    // only reads `.sequence`. Whole-index records always carry `index`.
    #[serde(default)]
    pub index: serde_json::Value,
}

/// Kind of a single delta item in the append-only served-index log. A write emits a
/// bounded set of these (the pages/objects it touched), so the log grows by O(delta)
/// per write instead of O(store). Follows the on-disk item taxonomy: a block item is a
/// concrete page-index entry change, an OBJECT item is an object-level change (e.g. a
/// TTL-only or whole-object tombstone), and a META item carries the compaction anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexItemKind {
    #[serde(rename = "page")]
    Page,
    #[serde(rename = "object")]
    Object,
    #[serde(rename = "meta")]
    Meta,
}

impl Default for IndexItemKind {
    fn default() -> Self {
        IndexItemKind::Page
    }
}

/// One delta item: the smallest change to the served index a write can produce. The
/// field set is the native Rust page-index projection -- `routing_bucket`/`page_ref_key`
/// locate the entry in the bucket map, and `address`/`size`/`in_log`/`deleted`/`model_id`/
/// `object_id`/`page_id` mirror the durable page metadata so replay reconstructs the same
/// `PageIndex` the whole-index serialization would have produced. `deleted` is a
/// tombstone: replaying it removes the entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexItem {
    #[serde(default)]
    pub kind: IndexItemKind,
    #[serde(default, rename = "routing_slot")]
    pub routing_bucket: u32,
    #[serde(default)]
    pub page_ref_key: String,
    #[serde(default)]
    pub object_key: String,
    #[serde(default)]
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(default)]
    pub object_id: u64,
    #[serde(default)]
    pub page_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<BlockAddress>,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub in_log: bool,
    #[serde(default)]
    pub deleted: bool,
}

/// Band/zone lifecycle state folded into the index-log MetaItem. 1:1 with
/// `block_store::BlockStoreBandState` and with the on-disk band-state encoding
/// (INIT/CREATED/FROZEN/RECYCLED): Active==CREATED, Sealed==FROZEN, DelayedDestroy/Purged
/// cover the RECYCLED grace. Serialized snake_case so it round-trips with the band manifest's
/// own state enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

/// One band/zone catalog entry folded into the index-log MetaItem, mirroring this design
/// the durable band catalog in the index-log anchor. Carries the DURABLE catalog fields the
/// band descriptor tracks -- lifecycle state, byte counts, timestamps, page-id range, version.
/// The band descriptor's DIAGNOSTIC fields (readable_prefix_physical_bytes / has_corruption /
/// first_error*) are intentionally ABSENT: they are recomputed on load by scanning the slab
/// (`inspect_slab`, driven by `rebuild_band_manifest_at` / reconcile-on-open), exactly as the
/// are not persisted. So this is the lossless durable projection of a band.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoneInfo {
    #[serde(alias = "zone_id")]
    pub page_slab_id: u64,
    pub state: ZoneState,
    #[serde(alias = "total_bytes")]
    pub physical_bytes: u64,
    #[serde(default)]
    pub logical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page_id: Option<u64>,
    #[serde(default)]
    pub version: u64,
}

/// Compaction anchor for the delta log. `start_wal_sequence` is the lowest WAL sequence
/// still required to reconstruct the served index on top of the base snapshot: once the
/// base `shard-{id}.index.json` is rewritten at a compaction point, the anchor advances
/// and every delta record at or before it can be truncated. Matches
/// MetaItem's `start_WAL_id` role in the native WAL vocabulary.
///
/// `zones` folds the band catalog into the anchor. It
/// is populated ONLY at a threshold dump, and ONLY when the `TS_INDEX_CATALOG_FOLD` gate is on;
/// with the gate off it is always empty and (via `skip_serializing_if`) not serialized, so an
/// anchor record is byte-identical to the pre-fold record. Legacy anchors without `zones`
/// deserialize to an empty catalog and replay unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaItem {
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub start_wal_sequence: u64,
    #[serde(default)]
    pub timestamp_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub zones: Vec<ZoneInfo>,
    #[serde(default)]
    pub zone_version: u64,
}

/// One appended delta record: either a batch of page/object item deltas (PAGE/OBJECT) or
/// a compaction anchor (META). Written one JSON line per record to the same append-only
/// log file as `IndexLogRecord`; the two are distinguished on read by the presence of the
/// `items`/`meta` fields, so legacy whole-index records replay untouched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDeltaRecord {
    pub shard_id: ShardId,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<IndexItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<MetaItem>,
    /// The WAL sequence this write reflected (the served-index anchor at append time). On
    /// load, deltas with a sequence at or below the base snapshot's anchor are already
    /// folded into the base and are skipped; folding the rest advances the reconstructed
    /// anchor so WAL replay re-executes only the uncaptured tail (never relocating the
    /// pages the deltas already pin at their original addresses).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_wal_sequence: Option<u64>,
    /// Opaque per-touched-key state blobs (one JSON object per key) carrying the
    /// authoritative post-write value of the maps that are NOT reconstructable from a
    /// single page-index entry -- packed timestamped series (feature membership survives
    /// eviction) and the non-page maps (TTL, control-state change/selection, context
    /// nodes). Opaque here so the index-log layer stays decoupled from `ShardState`; the
    /// engine builds and applies them. Replaying these on load pins the exact membership a
    /// write produced, so reconstruction from physical pages cannot resurrect evicted data.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_states: Vec<serde_json::Value>,
}

/// Fold an ordered stream of delta items onto a base map keyed by `(routing_bucket,
/// page_ref_key)`. Later items win; a `deleted` tombstone removes the key. This is the
/// pure replay step: `base` is the page-index projection of the base snapshot and the
/// returned map is the reconstructed, current page index -- O(base + deltas), and the
/// per-write producer side is O(delta).
pub fn fold_index_items(
    base: BTreeMap<(u32, String), IndexItem>,
    deltas: &[IndexItem],
) -> BTreeMap<(u32, String), IndexItem> {
    let mut folded = base;
    for item in deltas {
        let key = (item.routing_bucket, item.page_ref_key.clone());
        if item.deleted {
            folded.remove(&key);
        } else {
            folded.insert(key, item.clone());
        }
    }
    folded
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub last_sequence: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogGcReport {
    pub shard_id: ShardId,
    pub retain_from_sequence: u64,
    #[serde(default)]
    pub max_entries_per_round: usize,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    #[serde(default)]
    pub removable_records_before_budget: usize,
    #[serde(default)]
    pub budget_exhausted: bool,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

#[derive(Debug, Clone)]
pub struct LocalIndexLogStore {
    inner: Arc<Mutex<IndexLogInner>>,
    /// One barrier gate per shard log, so appends that arrive while an fsync is in flight ride it
    /// instead of queueing to take an identical one.
    flush_gates: Arc<crate::flush_gate::FlushRegistry>,
}

#[derive(Debug)]
struct IndexLogInner {
    root: PathBuf,
    stats: IndexLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
    /// MANIFEST-CONFORMANCE FOLD: the index-log byte length recorded at the last catalog dump, per
    /// shard. `undumped_len_since_dump` subtracts this from the current on-disk length to get the
    /// undumped gap that drives the threshold-dump cadence. Reset to 0
    /// on process restart, so the first post-restart cycle may dump once -- harmless (a dump only
    /// materializes durable state that is already recoverable).
    last_dumped_len_by_shard: HashMap<ShardId, u64>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<std::sync::Arc<crate::scratch::ScratchDirGuard>>,
}

fn indexlog_enabled() -> bool {
    // The index-log is the delta served-index's durable per-write delta stream: the
    // load-time fold reconstructs the served index (at the original page addresses) from
    // these records, so it must be written in ALL builds, not just tests. It is kept
    // bounded by GC-at-compaction, which truncates every delta already folded into the
    // durably-rewritten base snapshot (retain-from-anchor). Always on.
    true
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

/// Defer the ack-path index-log fsync (WAL replay is the durable recovery source). This is the
/// single-barrier default; restored to a synchronous fsync only under the TS_WAL_LEGACY_RECOVERY
/// escape hatch (whose delta-fold recovery trusts the durable delta).
fn indexlog_wal_only_sync() -> bool {
    !matches!(
        std::env::var("TS_WAL_LEGACY_RECOVERY")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// MANIFEST-CONFORMANCE FOLD gate (default OFF, byte-identical when off). When on, the band/zone
/// catalog is folded into the index-log anchor at a threshold dump
/// and the per-write band-manifest file stops being the
/// catalog's source of truth (it is reconstructed on load from the durable pages + the folded
/// anchor). Off, none of that fold code runs: no `zones` are ever captured (so anchor records
/// serialize identically), and recovery/persistence take the existing paths unchanged. Ships
/// dark; flips on after the crash-recovery suite is green.
pub fn index_catalog_fold_enabled() -> bool {
    matches!(
        std::env::var("TS_INDEX_CATALOG_FOLD")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Threshold decision for the background catalog/index dump, mirroring this design
/// index-meta dump gate (the dump-delay check compares the undumped
/// WAL length against the 1 MiB gap). `undumped_bytes` is the served-index-log growth since
/// the last dumped watermark; when it crosses `gap_bytes` the dump fires. A zero gap disables
/// the cadence (never dump on threshold) so an operator can pin dumps to compaction/unload only.
pub fn should_dump_index_catalog(undumped_bytes: u64, gap_bytes: u64) -> bool {
    gap_bytes > 0 && undumped_bytes >= gap_bytes
}

impl LocalIndexLogStore {
    /// On-disk byte length of a shard's index-log file (0 if absent). Used as the "undumped
    /// length" signal for the threshold-dump cadence: the growth of this file since the last
    /// dumped watermark is the undumped-length signal.
    pub fn log_len_bytes(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        index_log_path(&inner.root, shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    /// Undumped index-log length for a shard: the on-disk byte growth since the last catalog
    /// dump (`mark_catalog_dumped`). This is the native analog of this design
    /// the undumped WAL length, and is the signal compared against `index_dump_wal_gap_bytes`
    /// to decide a threshold dump. A shard never dumped this process (or freshly restarted)
    /// reports the whole current length.
    pub fn undumped_len_since_dump(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        let current = index_log_path(&inner.root, shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let dumped = inner
            .last_dumped_len_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        current.saturating_sub(dumped)
    }

    /// Record that a catalog dump captured the shard's index-log up to its current on-disk
    /// length, resetting the undumped gap to 0. Called by the engine ONLY after the dump's base
    /// index + folded anchor are durably written, so the watermark never advances past
    /// non-durable state (restart-during-dump re-dumps rather than skipping).
    pub fn mark_catalog_dumped(&self, shard_id: ShardId) {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let current = index_log_path(&inner.root, shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        inner.last_dumped_len_by_shard.insert(shard_id, current);
    }

    /// The most recent `MetaItem` anchor carrying a folded band/zone catalog, or `None` if no
    /// anchor with a non-empty `zones` list has been written. Used on load (gate on) to seed the
    /// block-store band catalog from the folded anchor when the band-manifest file is absent.
    pub fn latest_zone_catalog(&self, shard_id: ShardId) -> Result<Option<MetaItem>, IndexLogError> {
        let records = self.read_delta_records(shard_id, 0)?;
        Ok(records
            .into_iter()
            .filter_map(|record| record.meta)
            .filter(|meta| !meta.zones.is_empty())
            .next_back())
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(IndexLogInner {
                root,
                stats: IndexLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
                last_dumped_len_by_shard: HashMap::new(),
                scratch: None,
            })),
            flush_gates: Arc::new(crate::flush_gate::FlushRegistry::default()),
        }
    }

    pub fn append_json(
        &self,
        shard_id: ShardId,
        index_bytes: &[u8],
    ) -> Result<IndexLogRecord, IndexLogError> {
        // Bulk backfill: skip the replay-log append entirely (deferred to a
        // single flush of the served index). Removes the per-record fsync bomb.
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(IndexLogRecord {
                shard_id,
                sequence: 0,
                index: serde_json::Value::Null,
            });
        }
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        let next_sequence = last_sequence.saturating_add(1);
        let record = IndexLogRecord {
            shard_id,
            sequence: next_sequence,
            index: serde_json::from_slice(index_bytes)?,
        };
        // Frame the record with a length + SHA-256 digest (crate::log_framing) so a later
        // value-preserving bit-flip in this committed line is detected on read.
        let bytes = crate::log_framing::encode_line(&serde_json::to_vec(&record)?);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under the single-barrier default defer this fsync (bytes
        // still written): the WAL is the durable recovery source and replay rebuilds the
        // served index, so the replay-log checkpoint need not be crash-durable per write.
        if !indexlog_wal_only_sync() {
            file.sync_data()?;
        }
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(record)
    }

    pub fn append_index_bytes(
        &self,
        shard_id: ShardId,
        index_bytes: &[u8],
    ) -> Result<u64, IndexLogError> {
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(0);
        }
        debug_assert!(serde_json::from_slice::<serde_json::Value>(index_bytes).is_ok());
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        let next_sequence = last_sequence.saturating_add(1);
        // Build the JSON payload (no trailing newline) by splicing the pre-serialized index
        // bytes in directly (avoids re-parsing/re-encoding the whole index), then frame it
        // with a length + SHA-256 digest (crate::log_framing) for per-record integrity.
        let mut payload = Vec::with_capacity(index_bytes.len().saturating_add(96));
        write!(
            &mut payload,
            "{{\"shard_id\":{shard_id},\"sequence\":{next_sequence},\"index\":"
        )?;
        payload.extend_from_slice(index_bytes);
        payload.push(b'}');
        let bytes = crate::log_framing::encode_line(&payload);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under the single-barrier default defer this fsync (bytes
        // still written): the WAL is the durable recovery source and replay rebuilds the
        // served index, so the replay-log checkpoint need not be crash-durable per write.
        if !indexlog_wal_only_sync() {
            file.sync_data()?;
        }
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(next_sequence)
    }

    /// Append one O(delta) served-index record: the page/object items a single write
    /// touched, optionally carrying a compaction anchor. Unlike `append_json`, this never
    /// serializes the whole index -- the appended bytes are proportional to the change,
    /// which is what turns the per-write served-index persist cost from O(store) into
    /// O(delta). Returns the assigned monotonic sequence.
    ///
    /// `durable` controls the fsync: the normal (single-node / shared-store) path passes
    /// `true` (the record is fsync'd before returning). The raft apply path passes `false` --
    /// the record is written + flushed to the OS but NOT fsync'd, because there the raft log
    /// is the durability + reconstruction source and a lost non-fsync'd tail is rebuilt by
    /// raft-log replay on restart. The consumer-aware GC never truncates such a tail (it
    /// retains from the durable dump/cursor/snapshot frontier).
    pub fn append_delta(
        &self,
        shard_id: ShardId,
        items: Vec<IndexItem>,
        key_states: Vec<serde_json::Value>,
        applied_wal_sequence: Option<u64>,
        meta: Option<MetaItem>,
        durable: bool,
    ) -> Result<u64, IndexLogError> {
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(0);
        }
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        let next_sequence = last_sequence.saturating_add(1);
        let record = IndexDeltaRecord {
            shard_id,
            sequence: next_sequence,
            items,
            meta,
            applied_wal_sequence,
            key_states,
        };
        // Frame the delta record with a length + SHA-256 digest (crate::log_framing) so a
        // value-preserving bit-flip (e.g. a flipped `deleted` flag or page address) in this
        // committed line is detected on read rather than replayed as truth on recovery.
        let bytes = crate::log_framing::encode_line(&serde_json::to_vec(&record)?);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        // The bytes are in the file and the bookkeeping is done, so claim a barrier and then let
        // the lock go before taking it. Holding the lock across the fsync meant writers could
        // never reach the barrier together, so each paid for one that would have covered all of
        // them.
        let barrier = durable.then(|| {
            let gate = self.flush_gates.gate((shard_id as u64, 0));
            let ticket = gate.register_write();
            (gate, ticket)
        });
        drop(inner);
        if let Some((gate, ticket)) = barrier {
            gate.await_durable(ticket, || {
                crate::durability_metrics::record_barrier("engine_index_log_append");
                file.sync_data()
            })?;
        }
        Ok(next_sequence)
    }

    /// Read every delta record for a shard in log order. Records at or before
    /// `retain_after_sequence` (the base snapshot's anchor) are skipped, so replay applies
    /// only the suffix the base does not already reflect. Legacy whole-index
    /// (`IndexLogRecord`) lines are ignored here -- they are not delta records.
    pub fn read_delta_records(
        &self,
        shard_id: ShardId,
        retain_after_sequence: u64,
    ) -> Result<Vec<IndexDeltaRecord>, IndexLogError> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        let path = index_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut last_sequence = 0_u64;
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            // PROPAGATE decode/parse failures instead of silently skipping the line. Silently
            // dropping an unparseable interior delta record and continuing the fold advances
            // the reconstructed anchor past it, so an eviction/removal recorded ONLY in that
            // delta (not the WAL) is recovered from neither source = silent loss / dangling
            // ref. `decode_line` also verifies the per-record integrity envelope, so a
            // value-preserving bit-flip surfaces here as `Corruption`. Consistent with the
            // scan / last_sequence_at path, which already treats interior corruption as fatal.
            let payload = crate::log_framing::decode_line(line.as_bytes())?;
            let record: IndexDeltaRecord = serde_json::from_slice(payload)?;
            // Enforce delta sequence-continuity: sequences are assigned strictly monotonically
            // across ALL appended records (whole-index and delta share one counter), and GC
            // only truncates a leading prefix, so in file order each record's sequence must be
            // strictly greater than the previous. A drop below or duplicate means a lost /
            // reordered / corrupted record -- refuse rather than fold a holed delta stream.
            if record.sequence <= last_sequence {
                return Err(IndexLogError::Corruption(format!(
                    "index-log delta sequence continuity violation: record sequence {} is not greater than previous {}",
                    record.sequence, last_sequence
                )));
            }
            last_sequence = record.sequence;
            // A whole-index IndexLogRecord also deserializes into IndexDeltaRecord (its `index`
            // field is ignored, leaving the delta fields empty). Only keep records that carry a
            // delta payload OR a WAL anchor (an anchor-only record still advances the
            // reconstructed watermark on load).
            if record.items.is_empty()
                && record.meta.is_none()
                && record.key_states.is_empty()
                && record.applied_wal_sequence.is_none()
            {
                continue;
            }
            if record.sequence > retain_after_sequence {
                records.push(record);
            }
        }
        Ok(records)
    }

    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, IndexLogError> {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let path = index_log_path(&inner.root, shard_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; size as usize];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        inner.stats.reads += 1;
        inner.stats.bytes_read += read as u64;
        Ok(bytes)
    }

    pub fn scan(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IndexLogError> {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let path = index_log_path(&inner.root, shard_id);
        if !path.exists() {
            inner.stats.scans += 1;
            return Ok(Vec::new());
        }
        let _ = last_sequence_at(&inner.root, shard_id)?;
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(start_offset))?;
        let mut reader = BufReader::new(file);
        let mut offset = start_offset;
        let mut total = 0;
        let mut records = Vec::new();
        loop {
            let mut line = Vec::new();
            let read = reader.read_until(b'\n', &mut line)?;
            if read == 0 {
                break;
            }
            let next_offset = offset.saturating_add(read as u64);
            if next_offset > end_offset || total + read as u64 > max_bytes {
                break;
            }
            records.push((offset, line));
            offset = next_offset;
            total += read as u64;
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += total;
        Ok(records)
    }

    pub fn gc_before_sequence(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        self.gc_before_sequence_limited(shard_id, retain_from_sequence, 0)
    }

    pub fn gc_before_sequence_limited(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
        // Bounding this per round makes it WORSE. The round rewrites what it retains, so
        // removing fewer records means copying more of them: 40,000 records took 357 ms in one
        // unlimited round against 492 ms for a round limited to 200 -- dearer, and with the rest
        // still to do. Zero, which every caller passes, is the cheap option. See
        // `bounding_the_index_collector_per_round_costs_more_not_less`.
        max_entries_per_round: usize,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = index_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence,
                max_entries_per_round,
                ..IndexLogGcReport::default()
            });
        }

        let bytes_before = path.metadata()?.len();
        let _ = last_sequence_at(&inner.root, shard_id)?;
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut records_before = 0usize;
        let mut removed_this_round = 0usize;
        let mut removable_records_before_budget = 0usize;
        let mut retained = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records_before += 1;
            // Preserve the exact on-disk payload for retained records so a delta record is not
            // silently re-encoded as a whole-index record (which would drop its items/meta).
            // Decode verifies the integrity envelope; the retained raw payload is re-framed on
            // write-out below.
            let payload = crate::log_framing::decode_line(line.as_bytes())?;
            let record: IndexLogRecord = serde_json::from_slice(payload)?;
            if record.sequence < retain_from_sequence {
                removable_records_before_budget = removable_records_before_budget.saturating_add(1);
            }
            if record.sequence >= retain_from_sequence
                || (max_entries_per_round > 0 && removed_this_round >= max_entries_per_round)
            {
                // Retain the EXACT decoded payload, not a re-serialized IndexLogRecord: a delta
                // record also parses as IndexLogRecord (its delta fields are dropped), so
                // re-encoding the parsed struct would silently destroy retained delta items on
                // a GC round. Re-frame the untouched payload on write-out below.
                retained.push(payload.to_vec());
            } else {
                removed_this_round = removed_this_round.saturating_add(1);
            }
        }

        let temp_path = path.with_extension("jsonl.tmp");
        {
            let mut temp = File::create(&temp_path)?;
            for payload in &retained {
                temp.write_all(&crate::log_framing::encode_line(payload))?;
            }
            temp.flush()?;
            crate::durability_metrics::record_barrier("engine_index_log_gc");
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        let bytes_after = path.metadata()?.len();
        Ok(IndexLogGcReport {
            shard_id,
            retain_from_sequence,
            max_entries_per_round,
            records_before,
            records_after: retained.len(),
            records_removed: records_before.saturating_sub(retained.len()),
            removable_records_before_budget,
            budget_exhausted: max_entries_per_round > 0
                && removable_records_before_budget > max_entries_per_round,
            bytes_before,
            bytes_after,
        })
    }

    pub fn stats(&self, shard_id: ShardId) -> IndexLogStats {
        let inner = self.inner.lock().expect("index log lock poisoned");
        IndexLogStats {
            last_sequence: last_sequence_at(&inner.root, shard_id).unwrap_or_default(),
            ..inner.stats
        }
    }
}

impl Default for LocalIndexLogStore {
    fn default() -> Self {
        let scratch = crate::scratch::owned_scratch_dir("index-logs");
        let store = Self::new(scratch.path());
        store
            .inner
            .lock()
            .expect("index log lock poisoned")
            .scratch = Some(scratch);
        store
    }
}

fn index_log_path(root: &Path, shard_id: ShardId) -> PathBuf {
    root.join(format!("shard-{shard_id}.indexlog.jsonl"))
}

fn last_sequence_at(root: &Path, shard_id: ShardId) -> Result<u64, IndexLogError> {
    let path = index_log_path(root, shard_id);
    if !path.exists() {
        return Ok(0);
    }
    let file = OpenOptions::new().read(true).write(true).open(&path)?;
    let mut reader = BufReader::new(file.try_clone()?);
    let mut last = 0;
    let mut offset = 0_u64;
    let mut good_offset = 0_u64;
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        offset = offset.saturating_add(read as u64);
        if !line.ends_with(b"\n") {
            break;
        }
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            good_offset = offset;
            continue;
        }
        // A fully newline-terminated line that fails to parse is COMMITTED corruption, not a
        // torn tail (index-log records are single-line JSON with no embedded newline). Treating
        // it as end-of-log would set_len the file down to the last parseable record -- silently
        // dropping durable index-log records after the corrupt one AND rewinding the sequence
        // counter (the next append reuses a sequence that dump manifests already reference).
        // Surface it as an error (index-log replay returns DataLoss on a hole / digest
        // mismatch, never trims). A genuine torn tail lacks the trailing '\n' (break above).
        // `decode_line` verifies the per-record length + SHA-256 envelope (crate::log_framing)
        // and accepts legacy unframed records unchanged. Mirrors wal.rs::last_wal_sequence_at.
        let payload = crate::log_framing::decode_line(&line)?;
        let record = serde_json::from_slice::<IndexLogRecord>(payload)?;
        last = last.max(record.sequence);
        good_offset = offset;
    }
    if good_offset < offset || good_offset < file.metadata()?.len() {
        file.set_len(good_offset)?;
        crate::durability_metrics::record_barrier("engine_index_log_seq_probe");
        file.sync_all()?;
        sync_parent_dir(&path)?;
    }
    Ok(last)
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            crate::durability_metrics::record_barrier("engine_index_log_dir");
            dir.sync_all()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_store_scratch_dir_dies_with_the_last_clone() {
        let store = LocalIndexLogStore::default();
        let root = store.inner.lock().unwrap().root.clone();
        assert!(root.exists(), "Default must create its scratch dir");
        let clone = store.clone();
        drop(store);
        assert!(root.exists(), "a live clone must keep the scratch dir");
        drop(clone);
        assert!(!root.exists(), "the last clone's drop must remove the scratch dir");
    }

    #[test]
    fn gc_before_sequence_rewrites_index_log_with_retained_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for value in [1, 2, 3] {
            store
                .append_json(5, format!("{{\"value\":{value}}}").as_bytes())
                .unwrap();
        }

        let report = store.gc_before_sequence(5, 2).unwrap();
        assert_eq!(report.records_before, 3);
        assert_eq!(report.records_after, 2);
        assert_eq!(report.records_removed, 1);
        assert_eq!(store.stats(5).last_sequence, 3);
        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.stats(5).last_sequence, 3);
        assert_eq!(reopened.scan(5, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
        store.append_json(5, b"{\"value\":4}").unwrap();
        assert_eq!(store.stats(5).last_sequence, 4);
    }

    #[test]
    fn append_index_bytes_writes_parseable_index_log_record_without_reencoding_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let sequence = store.append_index_bytes(5, b"{\"value\":1}").unwrap();
        assert_eq!(sequence, 1);
        let rows = store.scan(5, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(rows.len(), 1);
        let payload = crate::log_framing::decode_line(&rows[0].1).unwrap();
        let record: IndexLogRecord = serde_json::from_slice(payload).unwrap();
        assert_eq!(record.shard_id, 5);
        assert_eq!(record.sequence, 1);
        assert_eq!(record.index, serde_json::json!({"value": 1}));
    }

    #[test]
    fn corrupt_tail_is_truncated_and_append_resumes_after_last_valid_index_log_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store.append_json(5, b"{\"value\":1}").unwrap();
        store.append_json(5, b"{\"value\":2}").unwrap();
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(index_log_path(dir.path(), 5))
                .unwrap();
            file.write_all(b"{\"shard_id\":5,\"sequence\":3").unwrap();
            file.sync_all().unwrap();
        }

        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.stats(5).last_sequence, 2);
        assert_eq!(reopened.scan(5, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
        let record = reopened.append_json(5, b"{\"value\":3}").unwrap();
        assert_eq!(record.sequence, 3);
        assert_eq!(reopened.scan(5, 0, u64::MAX, u64::MAX).unwrap().len(), 3);
    }

    fn page_item(bucket: u32, key: &str, deleted: bool) -> IndexItem {
        IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: bucket,
            page_ref_key: key.to_string(),
            object_key: key.to_string(),
            model_id: "m".to_string(),
            component: None,
            object_id: 1,
            page_id: 0,
            address: None,
            size: 8,
            in_log: false,
            deleted,
        }
    }

    #[test]
    fn append_delta_grows_log_by_only_the_changed_items() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let seq1 = store
            .append_delta(7, vec![page_item(1, "a", false)], Vec::new(), None, None, true)
            .unwrap();
        assert_eq!(seq1, 1);
        let seq2 = store
            .append_delta(7, vec![page_item(1, "b", false)], Vec::new(), None, None, true)
            .unwrap();
        assert_eq!(seq2, 2);
        // The two single-item deltas together are far smaller than a whole-index blob
        // would be, and each append wrote only its own item.
        let records = store.read_delta_records(7, 0).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].items.len(), 1);
        assert_eq!(records[1].items[0].page_ref_key, "b");
        // The sequence tail is readable across a reopen even though the log now holds
        // delta records rather than whole-index records.
        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.stats(7).last_sequence, 2);
    }

    #[test]
    fn fold_index_items_applies_tombstones_and_last_writer_wins() {
        let mut base = BTreeMap::new();
        base.insert((1u32, "a".to_string()), page_item(1, "a", false));
        base.insert((1u32, "b".to_string()), page_item(1, "b", false));
        // Delta: delete a, overwrite b, add c.
        let mut updated_b = page_item(1, "b", false);
        updated_b.size = 99;
        let deltas = vec![page_item(1, "a", true), updated_b, page_item(2, "c", false)];
        let folded = fold_index_items(base, &deltas);
        assert!(!folded.contains_key(&(1, "a".to_string())));
        assert_eq!(folded.get(&(1, "b".to_string())).unwrap().size, 99);
        assert!(folded.contains_key(&(2, "c".to_string())));
        assert_eq!(folded.len(), 2);
    }

    #[test]
    fn read_delta_records_skips_anchor_and_ignores_whole_index_lines() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        // A legacy whole-index record and a delta record share the log file.
        store.append_json(9, b"{\"value\":1}").unwrap();
        let anchor_seq = store
            .append_delta(9, vec![page_item(1, "a", false)], Vec::new(), None, None, true)
            .unwrap();
        store
            .append_delta(9, vec![page_item(1, "b", false)], Vec::new(), None, None, true)
            .unwrap();
        // Only delta records are returned; the whole-index line is ignored.
        let all = store.read_delta_records(9, 0).unwrap();
        assert_eq!(all.len(), 2);
        // Retaining after the first delta's sequence yields only the later delta.
        let suffix = store.read_delta_records(9, anchor_seq).unwrap();
        assert_eq!(suffix.len(), 1);
        assert_eq!(suffix[0].items[0].page_ref_key, "b");
    }

    #[test]
    fn append_delta_meta_anchor_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store
            .append_delta(3, vec![page_item(1, "a", false)], Vec::new(), None, None, true)
            .unwrap();
        let meta = MetaItem {
            version: 1,
            start_wal_sequence: 42,
            timestamp_ms: 100,
            ..MetaItem::default()
        };
        store.append_delta(3, Vec::new(), Vec::new(), None, Some(meta), true).unwrap();
        let records = store.read_delta_records(3, 0).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].meta.as_ref().unwrap().start_wal_sequence, 42);
    }

    #[test]
    fn interior_corruption_is_fatal_not_silent_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for i in 1..=4 {
            store
                .append_json(5, format!("{{\"value\":{i}}}").as_bytes())
                .unwrap();
        }
        drop(store);
        // Corrupt the 2nd record IN PLACE, keeping it newline-terminated with records 3 & 4
        // intact after it. A newline-terminated line that fails to parse is committed
        // corruption, not a torn tail.
        let path = index_log_path(dir.path(), 5);
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 4);
        let corrupted = format!(
            "{}\ncorrupt-not-json\n{}\n{}\n",
            lines[0], lines[2], lines[3]
        );
        std::fs::write(&path, corrupted).unwrap();
        // scan drives last_sequence_at, which must surface interior corruption as an error
        // rather than silently truncating records 3 & 4 and rewinding the sequence counter
        // (which durable dump manifests reference via index_log_sequence).
        let reopened = LocalIndexLogStore::new(dir.path());
        assert!(
            reopened.scan(5, 0, u64::MAX, u64::MAX).is_err(),
            "interior index-log corruption must be fatal, not silently truncated"
        );
    }

    #[test]
    fn read_delta_records_propagates_interior_delta_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for key in ["a", "b", "c"] {
            store
                .append_delta(5, vec![page_item(1, key, false)], Vec::new(), Some(1), None, true)
                .unwrap();
        }
        drop(store);
        // Corrupt the 2nd delta record in place, keeping it newline-terminated with the 3rd
        // intact after it. Previously read_delta_records `if let Ok(..)` SILENTLY SKIPPED such
        // a line and folded on -- losing a removal/eviction recorded only in that delta. It
        // must now propagate the error and abort.
        let path = index_log_path(dir.path(), 5);
        let contents = std::fs::read(&path).unwrap();
        let mut lines: Vec<Vec<u8>> = contents
            .split(|&byte| byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| line.to_vec())
            .collect();
        assert_eq!(lines.len(), 3);
        lines[1] = b"corrupt-not-a-record".to_vec();
        let mut rebuilt = Vec::new();
        for line in &lines {
            rebuilt.extend_from_slice(line);
            rebuilt.push(b'\n');
        }
        std::fs::write(&path, &rebuilt).unwrap();
        let reopened = LocalIndexLogStore::new(dir.path());
        assert!(
            reopened.read_delta_records(5, 0).is_err(),
            "an interior delta record that fails to decode must abort, not be silently skipped"
        );
    }

    #[test]
    fn read_delta_records_enforces_sequence_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for (index, key) in ["a", "b", "c"].iter().enumerate() {
            store
                .append_delta(
                    5,
                    vec![page_item(1, key, false)],
                    Vec::new(),
                    Some(index as u64 + 1),
                    None,
                    true,
                )
                .unwrap();
        }
        drop(store);
        // Reorder the (valid, framed) records so their sequences read 1, 3, 2 -- a continuity
        // violation that means a record was lost/reordered. Each line is individually valid
        // (correct digest), so this exercises the sequence-continuity guard, not the checksum.
        let path = index_log_path(dir.path(), 5);
        let contents = std::fs::read(&path).unwrap();
        let lines: Vec<Vec<u8>> = contents
            .split(|&byte| byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| line.to_vec())
            .collect();
        assert_eq!(lines.len(), 3);
        let mut rebuilt = Vec::new();
        for index in [0usize, 2, 1] {
            rebuilt.extend_from_slice(&lines[index]);
            rebuilt.push(b'\n');
        }
        std::fs::write(&path, &rebuilt).unwrap();
        let reopened = LocalIndexLogStore::new(dir.path());
        match reopened.read_delta_records(5, 0) {
            Err(IndexLogError::Corruption(_)) => {}
            other => panic!("out-of-order delta sequences must be a Corruption error, got {other:?}"),
        }
    }

    #[test]
    fn meta_item_without_zones_serializes_byte_identically_to_pre_fold() {
        // Byte-identical-when-off invariant: an anchor whose `zones` is empty (the only state
        // reachable with TS_INDEX_CATALOG_FOLD off) must serialize with NO `zones` key and NO
        // `zone_version` beyond what a pre-fold MetaItem produced. `zone_version` defaults to 0
        // and is not skipped, so it appears; assert the value carries only the legacy three
        // fields plus a zero zone_version and no zones array.
        let meta = MetaItem {
            version: 7,
            start_wal_sequence: 11,
            timestamp_ms: 22,
            ..MetaItem::default()
        };
        let value = serde_json::to_value(&meta).unwrap();
        let object = value.as_object().unwrap();
        assert!(!object.contains_key("zones"), "empty zones must be skipped");
        assert_eq!(object.get("version").unwrap(), 7);
        assert_eq!(object.get("start_wal_sequence").unwrap(), 11);
        assert_eq!(object.get("timestamp_ms").unwrap(), 22);
        assert_eq!(object.get("zone_version").unwrap(), 0);
        // And it round-trips.
        let back: MetaItem = serde_json::from_value(value).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn meta_item_zone_catalog_round_trips_through_the_delta_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let meta = MetaItem {
            version: 1,
            start_wal_sequence: 5,
            timestamp_ms: 100,
            zone_version: 3,
            zones: vec![
                ZoneInfo {
                    page_slab_id: 0,
                    state: ZoneState::Sealed,
                    physical_bytes: 4096,
                    logical_bytes: 4000,
                    created_unix_ms: Some(10),
                    updated_unix_ms: Some(20),
                    first_page_id: Some(0),
                    last_page_id: Some(9),
                    version: 3,
                },
                ZoneInfo {
                    page_slab_id: 1,
                    state: ZoneState::Active,
                    physical_bytes: 512,
                    logical_bytes: 512,
                    created_unix_ms: Some(30),
                    updated_unix_ms: Some(30),
                    first_page_id: Some(10),
                    last_page_id: Some(10),
                    version: 3,
                },
            ],
        };
        store
            .append_delta(4, Vec::new(), Vec::new(), Some(5), Some(meta.clone()), true)
            .unwrap();
        // A reopen reads the folded catalog back exactly, and latest_zone_catalog finds it.
        let reopened = LocalIndexLogStore::new(dir.path());
        let recovered = reopened.latest_zone_catalog(4).unwrap().unwrap();
        assert_eq!(recovered, meta);
        assert_eq!(recovered.zones.len(), 2);
        assert_eq!(recovered.zones[0].state, ZoneState::Sealed);
        assert_eq!(recovered.zones[1].page_slab_id, 1);
    }

    #[test]
    fn latest_zone_catalog_prefers_the_newest_anchor_and_ignores_empty_ones() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let older = MetaItem {
            version: 1,
            start_wal_sequence: 1,
            timestamp_ms: 1,
            zone_version: 1,
            zones: vec![ZoneInfo {
                page_slab_id: 0,
                state: ZoneState::Active,
                physical_bytes: 1,
                logical_bytes: 1,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_page_id: None,
                last_page_id: None,
                version: 1,
            }],
        };
        let newer = MetaItem {
            version: 2,
            start_wal_sequence: 9,
            timestamp_ms: 9,
            zone_version: 2,
            zones: vec![ZoneInfo {
                page_slab_id: 0,
                state: ZoneState::Sealed,
                physical_bytes: 2,
                logical_bytes: 2,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_page_id: None,
                last_page_id: None,
                version: 2,
            }],
        };
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(1), Some(older), true)
            .unwrap();
        // An anchor with no zones between them must not shadow the folded catalog.
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(5), Some(MetaItem::default()), true)
            .unwrap();
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(9), Some(newer.clone()), true)
            .unwrap();
        assert_eq!(store.latest_zone_catalog(6).unwrap().unwrap(), newer);
    }

    #[test]
    fn should_dump_index_catalog_fires_only_past_the_gap() {
        assert!(!should_dump_index_catalog(0, 1024));
        assert!(!should_dump_index_catalog(1023, 1024));
        assert!(should_dump_index_catalog(1024, 1024));
        assert!(should_dump_index_catalog(4096, 1024));
        // A zero gap disables the threshold cadence entirely.
        assert!(!should_dump_index_catalog(u64::MAX, 0));
    }

    #[test]
    fn legacy_unframed_index_log_still_loads_after_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        // Pre-upgrade whole-index records: raw single-line JSON, no framing.
        let path = index_log_path(dir.path(), 8);
        let raw = b"{\"shard_id\":8,\"sequence\":1,\"index\":{\"v\":1}}\n{\"shard_id\":8,\"sequence\":2,\"index\":{\"v\":2}}\n";
        std::fs::write(&path, raw).unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        assert_eq!(store.stats(8).last_sequence, 2);
        assert_eq!(store.scan(8, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
        // A new append is framed; the mixed file still loads and continues the sequence.
        let record = store.append_json(8, b"{\"v\":3}").unwrap();
        assert_eq!(record.sequence, 3);
        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.scan(8, 0, u64::MAX, u64::MAX).unwrap().len(), 3);
        assert_eq!(reopened.stats(8).last_sequence, 3);
    }

    /// Bounding this collector per round costs MORE, not less.
    ///
    /// Every other sweep here is bounded per round, this one takes a limit, and the production
    /// path never passes one -- which looks exactly like an oversight worth fixing. It is not.
    ///
    /// The collector rewrites what it KEEPS: retained records are copied into a fresh file. So a
    /// round that removes fewer records retains more and copies more. Timed on one round, removing
    /// everything but the last record: 2,000 records took 22.7 ms unlimited against 29.0 ms
    /// limited to 200; 40,000 took 356.7 ms against 492.0 ms. The bounded round is dearer and
    /// leaves the rest of the work for later rounds that are dearer still.
    ///
    /// The assertion is on bytes rewritten rather than time, because that is the thing that makes
    /// it true and it does not depend on the machine.
    #[test]
    fn bounding_the_index_collector_per_round_costs_more_not_less() {
        let records = 4_000usize;
        let mut rewritten = Vec::new();
        for limit in [0usize, 200] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalIndexLogStore::new(dir.path());
            for value in 0..records {
                store
                    .append_json(5, format!("{{\"value\":{value}}}").as_bytes())
                    .unwrap();
            }
            let report = store
                .gc_before_sequence_limited(5, records as u64, limit)
                .unwrap();
            // What the round costs is what it copies, which is what it retained.
            rewritten.push((limit, report.records_removed, report.bytes_after));
        }

        let (_, unlimited_removed, unlimited_bytes) = rewritten[0];
        let (_, limited_removed, limited_bytes) = rewritten[1];
        assert_eq!(unlimited_removed, records - 1, "unlimited should clear the log");
        assert_eq!(limited_removed, 200, "the limit should be respected");
        assert!(
            limited_bytes > unlimited_bytes * 10,
            "a bounded round should be shown rewriting far more than an unbounded one \
             ({limited_bytes} bytes against {unlimited_bytes}); if that is no longer true the \
             collector has stopped copying what it keeps, and a per-round bound may now be worth \
             having"
        );
    }

    /// Concurrent appends to one shard's index log must SHARE durability barriers.
    ///
    /// An fsync makes every byte already in the file durable, so a barrier taken while other
    /// writers are queued behind it covers them too. This held the store lock across the fsync,
    /// so writers could not reach the barrier together and each paid for one of its own -- the
    /// same shape the raft node log had.
    #[test]
    fn concurrent_index_log_appends_share_barriers() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(LocalIndexLogStore::new(dir.path()));
        let writers = 16usize;
        let each = 8usize;
        // Release the threads together, so they genuinely overlap rather than trickling through
        // one at a time and each finding no barrier to ride.
        let start = std::sync::Arc::new(std::sync::Barrier::new(writers));

        crate::durability_metrics::reset();
        let handles: Vec<_> = (0..writers)
            .map(|writer| {
                let store = std::sync::Arc::clone(&store);
                let start = std::sync::Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for index in 0..each {
                        store
                            .append_delta(
                                4,
                                vec![page_item(1, &format!("k-{writer}-{index}"), false)],
                                Vec::new(),
                                None,
                                None,
                                true,
                            )
                            .unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let appends = (writers * each) as u64;
        let barriers = crate::durability_metrics::snapshot()
            .get("engine_index_log_append")
            .copied()
            .unwrap_or(0);
        assert!(barriers >= 1, "some barrier must actually be taken");
        assert!(
            barriers < appends,
            "{barriers} barriers for {appends} concurrent appends -- nothing coalesced"
        );

        // Sharing a barrier must cost nobody their record: every append is still readable.
        let records = store.read_delta_records(4, 0).unwrap();
        assert_eq!(records.len() as u64, appends, "every append must survive");
    }
}
