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
/// per write instead of O(store). Mirrors the on-disk item taxonomy: a PAGE item is a
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

/// Compaction anchor for the delta log. `start_wal_sequence` is the lowest WAL sequence
/// still required to reconstruct the served index on top of the base snapshot: once the
/// base `shard-{id}.index.json` is rewritten at a compaction point, the anchor advances
/// and every delta record at or before it can be truncated. Mirrors the reference
/// MetaItem's `start_oplog_id` role in the native WAL vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaItem {
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub start_wal_sequence: u64,
    #[serde(default)]
    pub timestamp_ms: u64,
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
}

#[derive(Debug)]
struct IndexLogInner {
    root: PathBuf,
    stats: IndexLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
}

fn indexlog_enabled() -> bool {
    // Replay-only index-log is opt-in at runtime (default off) so it can't grow
    // unbounded and fill the disk; always on under test so checkpoint/manifest
    // logic and the low-level unit tests keep exercising it.
    cfg!(test)
        || matches!(
            std::env::var("MATRIXARK_ENABLE_INDEXLOG")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        )
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

/// TS_WAL_ONLY_SYNC: defer the ack-path index-log fsync (WAL replay is the durable
/// recovery source). Default OFF.
fn indexlog_wal_only_sync() -> bool {
    matches!(
        std::env::var("TS_WAL_ONLY_SYNC")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// TS_INDEXLOG_DEFER_SYNC: drop the served-index delta-log fdatasync from the synchronous
/// write/ack path. The delta record is still APPENDED (so the served-index stream and every
/// consumer of it -- gc / reclaim / manifest / reconcile / cold reload -- are unchanged); only
/// its fdatasync is deferred. Combined with TS_WAL_ONLY_SYNC this takes the ack path from three
/// synchronous barriers (WAL + data-page + index-log) to two: the WAL and the data pages stay
/// durable per write, so no index reference can dangle at an un-synced page, and the durable WAL
/// rebuilds any lost index-log delta tail on replay. Default OFF.
///
/// NOTE: this stops at two barriers on purpose. Eviction / expiry / compaction decisions are
/// recorded ONLY in the served-index delta, not in the WAL, so a pure WAL replay would resurrect
/// points a background op had removed. Reaching one barrier requires making those served-index-
/// only mutations WAL-reconstructable first; that is a separate, larger change.
fn indexlog_defer_sync() -> bool {
    matches!(
        std::env::var("TS_INDEXLOG_DEFER_SYNC")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl LocalIndexLogStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(IndexLogInner {
                root,
                stats: IndexLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
            })),
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
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under TS_WAL_ONLY_SYNC defer this fsync (bytes are
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
        let mut bytes = Vec::with_capacity(index_bytes.len().saturating_add(96));
        write!(
            &mut bytes,
            "{{\"shard_id\":{shard_id},\"sequence\":{next_sequence},\"index\":"
        )?;
        bytes.extend_from_slice(index_bytes);
        bytes.extend_from_slice(b"}\n");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under TS_WAL_ONLY_SYNC defer this fsync (bytes are
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
    pub fn append_delta(
        &self,
        shard_id: ShardId,
        items: Vec<IndexItem>,
        key_states: Vec<serde_json::Value>,
        applied_wal_sequence: Option<u64>,
        meta: Option<MetaItem>,
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
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path delta append. Under TS_INDEXLOG_DEFER_SYNC the record is written but its
        // fdatasync is deferred: the WAL + data pages are still durable per write, so the lost
        // delta tail is rebuilt by WAL replay and no page reference dangles. This drops the
        // index-log fdatasync from the write critical path (three barriers -> two).
        if !indexlog_defer_sync() {
            file.sync_data()?;
        }
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
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
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(record) = serde_json::from_str::<IndexDeltaRecord>(&line) {
                // A whole-index IndexLogRecord also deserializes into IndexDeltaRecord
                // (its `index` field is ignored, leaving the delta fields empty). Only keep
                // records that carry a delta payload OR a WAL anchor (an anchor-only record
                // still advances the reconstructed watermark on load).
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
        let _ = last_sequence_at(&inner.root, shard_id)?;
        let path = index_log_path(&inner.root, shard_id);
        let mut file = File::open(path)?;
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
            let record: IndexLogRecord = serde_json::from_str(&line)?;
            if record.sequence < retain_from_sequence {
                removable_records_before_budget = removable_records_before_budget.saturating_add(1);
            }
            if record.sequence >= retain_from_sequence
                || (max_entries_per_round > 0 && removed_this_round >= max_entries_per_round)
            {
                retained.push(record);
            } else {
                removed_this_round = removed_this_round.saturating_add(1);
            }
        }

        let temp_path = path.with_extension("jsonl.tmp");
        {
            let mut temp = File::create(&temp_path)?;
            for record in &retained {
                serde_json::to_writer(&mut temp, record)?;
                temp.write_all(b"\n")?;
            }
            temp.flush()?;
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
        Self::new(unique_temp_path("index-logs"))
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
        // Surface it as an error (index-log replay returns DataLoss on a hole / CRC
        // mismatch, never trims). A genuine torn tail lacks the trailing '\n' (break above).
        // Mirrors the WAL fix in wal.rs::last_wal_sequence_at.
        let record = serde_json::from_slice::<IndexLogRecord>(&line)?;
        last = last.max(record.sequence);
        good_offset = offset;
    }
    if good_offset < offset || good_offset < file.metadata()?.len() {
        file.set_len(good_offset)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
    }
    Ok(last)
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            dir.sync_all()?;
        }
    }
    Ok(())
}

fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let record: IndexLogRecord = serde_json::from_slice(&rows[0].1).unwrap();
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
            .append_delta(7, vec![page_item(1, "a", false)], Vec::new(), None, None)
            .unwrap();
        assert_eq!(seq1, 1);
        let seq2 = store
            .append_delta(7, vec![page_item(1, "b", false)], Vec::new(), None, None)
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
            .append_delta(9, vec![page_item(1, "a", false)], Vec::new(), None, None)
            .unwrap();
        store
            .append_delta(9, vec![page_item(1, "b", false)], Vec::new(), None, None)
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
            .append_delta(3, vec![page_item(1, "a", false)], Vec::new(), None, None)
            .unwrap();
        let meta = MetaItem {
            version: 1,
            start_wal_sequence: 42,
            timestamp_ms: 100,
        };
        store.append_delta(3, Vec::new(), Vec::new(), None, Some(meta)).unwrap();
        let records = store.read_delta_records(3, 0).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].meta.unwrap().start_wal_sequence, 42);
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
}
