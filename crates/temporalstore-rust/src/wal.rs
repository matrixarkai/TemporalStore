// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{Command, ShardId};

#[derive(Debug, Error)]
pub enum WriteAheadLogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteAheadLogRecord {
    pub shard_id: ShardId,
    pub sequence: u64,
    pub command: Command,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<WriteAheadLogRecordMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteAheadLogRecordMetadata {
    pub version: u32,
    pub timestamp_ms: u64,
    pub items: Vec<WriteAheadLogItemMetadata>,
}

impl WriteAheadLogRecordMetadata {
    pub fn single_command(command: &Command) -> Self {
        Self {
            version: WRITE_AHEAD_LOG_FORMAT_VERSION,
            timestamp_ms: current_time_ms(),
            items: vec![WriteAheadLogItemMetadata::from_command(command)],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteAheadLogItemMetadata {
    pub item_kind: WriteAheadLogItemKind,
    pub model: WriteAheadLogModel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "slot_id")]
    pub bucket_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub meta_log: bool,
    #[serde(default)]
    pub block_log: bool,
}

impl WriteAheadLogItemMetadata {
    pub fn from_command(command: &Command) -> Self {
        let object_key = command_object_key(command);
        let bucket_id = object_key.as_deref().map(command_bucket_id);
        let item_kind = command_item_kind(command);
        Self {
            item_kind,
            model: command_model(command),
            object_key,
            bucket_id,
            object_id: None,
            block_id: None,
            ttl_ms: command_ttl_ms(command),
            deleted: matches!(
                command,
                Command::CommonDelete { .. }
                    | Command::StringDelete { .. }
                    | Command::HashDelete { .. }
                    | Command::SetRemove { .. }
                    | Command::FeatureDelete { .. }
            ),
            meta_log: matches!(
                command,
                Command::CommonExpire { .. } | Command::CommonTtl { .. }
            ),
            block_log: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteAheadLogItemKind {
    Kv,
    Block,
    Ttl,
    DeleteObject,
    Query,
    Admin,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteAheadLogModel {
    Common,
    String,
    Hash,
    Set,
    Feature,
    Sequence,
    ControlState,
    Context,
    Admin,
    Unknown,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub flushes: u64,
    pub syncs: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub last_sequence: u64,
    pub last_flushed_sequence: u64,
    pub persistent_bytes: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogGcReport {
    pub shard_id: ShardId,
    pub retain_from_sequence: u64,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogFlushReport {
    pub shard_id: ShardId,
    pub path: PathBuf,
    pub last_sequence: u64,
    pub persistent_bytes: u64,
    pub synced: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogAppendReport {
    pub shard_id: ShardId,
    pub requested_sequence: u64,
    pub current_sequence: u64,
    pub appended: bool,
    pub offset: u64,
    pub size: u64,
    pub persistent_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogInfo {
    pub shard_id: ShardId,
    pub path: PathBuf,
    pub start_sequence: u64,
    pub current_sequence: u64,
    pub records: usize,
    pub length_bytes: u64,
    pub persistent_length_bytes: u64,
    pub last_flushed_sequence: u64,
    pub format_version: u32,
}

pub const WRITE_AHEAD_LOG_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct LocalWriteAheadLogStore {
    inner: Arc<Mutex<WriteAheadLogInner>>,
}

pub type WalError = WriteAheadLogError;
pub type WalRecord = WriteAheadLogRecord;
pub type WalStats = WriteAheadLogStats;
pub type WalGcReport = WriteAheadLogGcReport;
pub type LocalWalStore = LocalWriteAheadLogStore;

#[derive(Debug)]
struct WriteAheadLogInner {
    root: PathBuf,
    stats: WriteAheadLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
}

impl LocalWriteAheadLogStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(WriteAheadLogInner {
                root,
                stats: WriteAheadLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
            })),
        }
    }

    pub fn append(
        &self,
        shard_id: ShardId,
        command: Command,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        self.append_with_sync(shard_id, command, !wal_bulk_relaxed_durability())
    }

    /// Append a WAL record. `sync=true` fsyncs before returning (durable);
    /// `sync=false` writes the record but defers the fsync. Mirrors the WAL
    /// writer: StringModel::SetValue ALWAYS records the entry; EVENT_REPLICATION_SYNC
    /// vs ASYNC_STORAGE only changes whether the commit blocks
    /// The WAL commit runs synchronously vs deferred.
    pub fn append_with_sync(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let _append_lock = WalAppendLock::acquire(&inner.root, shard_id)?;
        let disk_last_sequence = last_wal_sequence_at(&inner.root, shard_id)?;
        let cached_last_sequence = inner
            .last_sequence_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or_default();
        let last_sequence = cached_last_sequence.max(disk_last_sequence);
        inner.last_sequence_by_shard.insert(shard_id, last_sequence);
        let next_sequence = last_sequence.saturating_add(1);
        let record = WriteAheadLogRecord {
            shard_id,
            sequence: next_sequence,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
            command,
        };
        let report = append_record_locked(&mut inner, &record, sync)?;
        inner.stats.last_sequence = report.current_sequence;
        // last_flushed_sequence is advanced by append_record_locked ONLY when the record was
        // actually fsynced (sync=true). An unconditional overwrite here reported an async /
        // bulk-mode (unsynced) record as durable -- overstating durability, a latent trap for
        // any future reclaim/ack gate that reads it. Let the flush-gated path own it.
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(record)
    }

    pub fn append_replayed_record(
        &self,
        record: WriteAheadLogRecord,
    ) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&record.shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_wal_sequence_at(&inner.root, record.shard_id)?;
                inner
                    .last_sequence_by_shard
                    .insert(record.shard_id, sequence);
                sequence
            }
        };
        if record.sequence <= last_sequence {
            let path = write_ahead_log_path(&inner.root, record.shard_id);
            return Ok(WriteAheadLogAppendReport {
                shard_id: record.shard_id,
                requested_sequence: record.sequence,
                current_sequence: last_sequence,
                appended: false,
                offset: path.metadata().map(|metadata| metadata.len()).unwrap_or(0),
                size: 0,
                persistent_bytes: path.metadata().map(|metadata| metadata.len()).unwrap_or(0),
            });
        }
        let report = append_record_locked(&mut inner, &record, true)?;
        inner
            .last_sequence_by_shard
            .insert(record.shard_id, record.sequence);
        inner.stats.last_sequence = record.sequence;
        inner.stats.last_flushed_sequence = record.sequence;
        Ok(report)
    }

    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
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
    ) -> Result<Vec<(u64, Vec<u8>)>, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let _ = last_wal_sequence_at(&inner.root, shard_id)?;
        let path = write_ahead_log_path(&inner.root, shard_id);
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

    pub fn flush(&self, shard_id: ShardId) -> Result<WriteAheadLogFlushReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = write_ahead_log_path(&inner.root, shard_id);
        let last_sequence = last_wal_sequence_at(&inner.root, shard_id)?;
        if !path.exists() {
            return Ok(WriteAheadLogFlushReport {
                shard_id,
                path,
                last_sequence,
                persistent_bytes: 0,
                synced: false,
            });
        }
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
        let persistent_bytes = path.metadata()?.len();
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = last_sequence;
        inner.stats.persistent_bytes = persistent_bytes;
        Ok(WriteAheadLogFlushReport {
            shard_id,
            path,
            last_sequence,
            persistent_bytes,
            synced: true,
        })
    }

    pub fn gc_before_sequence(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> Result<WriteAheadLogGcReport, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = write_ahead_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(WriteAheadLogGcReport {
                shard_id,
                retain_from_sequence,
                ..WriteAheadLogGcReport::default()
            });
        }

        let bytes_before = path.metadata()?.len();
        let last_sequence = last_wal_sequence_at(&inner.root, shard_id)?;
        // Never delete the highest-sequence record. The WAL file is the sequence-generator
        // source on restart (append seeds last_sequence_by_shard from last_wal_sequence_at), so
        // emptying it entirely on a full reclaim (retain_from > max) would regress the next
        // append to sequence 1 -> sequence REUSE + silent loss: the re-used seq is <= the
        // persisted applied_wal_sequence anchor, so replay's `sequence > watermark` filter drops
        // it. the zone-aligned wal Truncate always retains the tail zone holding the highest
        // sequence for exactly this continuity reason. Clamp the retain floor to keep the tail.
        let effective_retain = retain_from_sequence.min(last_sequence);
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut records_before = 0usize;
        let mut retained = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records_before += 1;
            let record: WriteAheadLogRecord = serde_json::from_str(&line)?;
            if record.sequence >= effective_retain {
                retained.push(record);
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
        Ok(WriteAheadLogGcReport {
            shard_id,
            retain_from_sequence,
            records_before,
            records_after: retained.len(),
            records_removed: records_before.saturating_sub(retained.len()),
            bytes_before,
            bytes_after,
        })
    }

    pub fn stats(&self, shard_id: ShardId) -> WriteAheadLogStats {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        WriteAheadLogStats {
            last_sequence: last_wal_sequence_at(&inner.root, shard_id).unwrap_or_default(),
            persistent_bytes: path.metadata().map(|metadata| metadata.len()).unwrap_or(0),
            ..inner.stats
        }
    }

    pub fn info(&self, shard_id: ShardId) -> Result<WriteAheadLogInfo, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(WriteAheadLogInfo {
                shard_id,
                path,
                format_version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                ..WriteAheadLogInfo::default()
            });
        }
        let _ = last_wal_sequence_at(&inner.root, shard_id)?;
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut start_sequence = 0_u64;
        let mut current_sequence = 0_u64;
        let mut records = 0_usize;
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record: WriteAheadLogRecord = serde_json::from_str(&line)?;
            if start_sequence == 0 {
                start_sequence = record.sequence;
            }
            current_sequence = current_sequence.max(record.sequence);
            records += 1;
        }
        let length_bytes = path.metadata()?.len();
        Ok(WriteAheadLogInfo {
            shard_id,
            path,
            start_sequence,
            current_sequence,
            records,
            length_bytes,
            persistent_length_bytes: length_bytes,
            last_flushed_sequence: inner.stats.last_flushed_sequence.max(current_sequence),
            format_version: WRITE_AHEAD_LOG_FORMAT_VERSION,
        })
    }
}

impl Default for LocalWriteAheadLogStore {
    fn default() -> Self {
        Self::new(unique_temp_path("wals"))
    }
}

fn write_ahead_log_path(root: &Path, shard_id: ShardId) -> PathBuf {
    root.join(format!("shard-{shard_id}.wal.jsonl"))
}

struct WalAppendLock {
    #[allow(dead_code)]
    file: File,
}

impl WalAppendLock {
    fn acquire(root: &Path, shard_id: ShardId) -> Result<Self, WriteAheadLogError> {
        fs::create_dir_all(root)?;
        let path = root.join(format!("shard-{shard_id}.wal.lock"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        lock_file_exclusive(&file)?;
        Ok(Self { file })
    }
}

impl Drop for WalAppendLock {
    fn drop(&mut self) {
        let _ = unlock_file(&self.file);
    }
}

#[cfg(unix)]
fn lock_file_exclusive(file: &File) -> Result<(), std::io::Error> {
    const LOCK_EX: i32 = 2;
    loop {
        let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX) };
        if rc == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

#[cfg(unix)]
fn unlock_file(file: &File) -> Result<(), std::io::Error> {
    const LOCK_UN: i32 = 8;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
unsafe extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[cfg(not(unix))]
fn lock_file_exclusive(_file: &File) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(not(unix))]
fn unlock_file(_file: &File) -> Result<(), std::io::Error> {
    Ok(())
}

fn wal_bulk_relaxed_durability() -> bool {
    matches!(
        std::env::var("MATRIXARK_BULK_INGEST")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn append_record_locked(
    inner: &mut WriteAheadLogInner,
    record: &WriteAheadLogRecord,
    sync: bool,
) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
    let path = write_ahead_log_path(&inner.root, record.shard_id);
    let offset = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    file.write_all(&bytes)?;
    if sync {
        file.flush()?;
        file.sync_data()?;
        sync_parent_dir(&path)?;
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = record.sequence;
    }
    let persistent_bytes = path.metadata()?.len();
    inner.stats.writes += 1;
    inner.stats.bytes_written += bytes.len() as u64;
    inner.stats.persistent_bytes = persistent_bytes;
    Ok(WriteAheadLogAppendReport {
        shard_id: record.shard_id,
        requested_sequence: record.sequence,
        current_sequence: record.sequence,
        appended: true,
        offset,
        size: bytes.len() as u64,
        persistent_bytes,
    })
}

fn last_wal_sequence_at(root: &Path, shard_id: ShardId) -> Result<u64, WriteAheadLogError> {
    let path = write_ahead_log_path(root, shard_id);
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
        // torn tail: WAL records are single-line JSON with no embedded newline, so a complete
        // (\n-terminated) line is a complete write. Surface it as an error instead of breaking
        // -- treating interior corruption as end-of-log would set_len the file down to the last
        // parseable record, silently dropping every durable record after the corrupt one and
        // defeating the strict replay-continuity DataLoss guard (ReplayWal returns
        // DataLoss on a CRC failure / hole rather than trimming). A genuine torn tail lacks the
        // trailing '\n' and is still handled by the break above.
        let record = serde_json::from_slice::<WriteAheadLogRecord>(&line)?;
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

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn command_variant_name(command: &Command) -> Option<String> {
    let value = serde_json::to_value(command).ok()?;
    let object = value.as_object()?;
    object
        .get("kind")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}

fn command_payload(command: &Command) -> Option<serde_json::Map<String, serde_json::Value>> {
    let value = serde_json::to_value(command).ok()?;
    let object = value.as_object()?;
    let mut payload = object.clone();
    payload.remove("kind");
    Some(payload)
}

fn command_object_key(command: &Command) -> Option<String> {
    let payload = command_payload(command)?;
    if let Some(key) = payload.get("key").and_then(|value| value.as_str()) {
        return Some(key.to_string());
    }
    let tenant = payload.get("tenant_hash").and_then(|value| value.as_u64());
    let node = payload
        .get("node_hash")
        .or_else(|| payload.get("parent_hash"))
        .or_else(|| payload.get("start_node_hash"))
        .and_then(|value| value.as_u64());
    match (tenant, node) {
        (Some(tenant), Some(node)) => Some(format!("context:{tenant}:{node}")),
        (Some(tenant), None) => Some(format!("context:{tenant}")),
        _ => None,
    }
}

fn command_ttl_ms(command: &Command) -> Option<u64> {
    let payload = command_payload(command)?;
    payload.get("ttl_ms").and_then(|value| value.as_u64())
}

fn command_bucket_id(key: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash & 0x3fff
}

fn command_model(command: &Command) -> WriteAheadLogModel {
    let Some(name) = command_variant_name(command) else {
        return WriteAheadLogModel::Unknown;
    };
    if name.starts_with("common_") {
        WriteAheadLogModel::Common
    } else if name.starts_with("string_") {
        WriteAheadLogModel::String
    } else if name.starts_with("hash_") {
        WriteAheadLogModel::Hash
    } else if name.starts_with("set_") {
        WriteAheadLogModel::Set
    } else if name.starts_with("feature_") {
        WriteAheadLogModel::Feature
    } else if name.starts_with("sequence_") {
        WriteAheadLogModel::Sequence
    } else if name.starts_with("control_state_") {
        WriteAheadLogModel::ControlState
    } else if name.starts_with("context_") {
        WriteAheadLogModel::Context
    } else {
        WriteAheadLogModel::Unknown
    }
}

fn command_item_kind(command: &Command) -> WriteAheadLogItemKind {
    let Some(name) = command_variant_name(command) else {
        return WriteAheadLogItemKind::Admin;
    };
    if name.contains("delete") || name.contains("remove") {
        WriteAheadLogItemKind::DeleteObject
    } else if name.contains("expire") || name.ends_with("ttl") {
        WriteAheadLogItemKind::Ttl
    } else if name.contains("get")
        || name.contains("query")
        || name.contains("count")
        || name.contains("stat")
        || name.contains("debug")
        || name.contains("manager")
        || name.contains("members")
        || name.contains("exists")
    {
        WriteAheadLogItemKind::Query
    } else {
        WriteAheadLogItemKind::Kv
    }
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
    use crate::types::Command;

    #[test]
    fn wal_interleaved_processes_refresh_sequence_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let first = LocalWriteAheadLogStore::new(dir.path());
        let second = LocalWriteAheadLogStore::new(dir.path());

        let one = first
            .append(
                1,
                Command::StringSet {
                    key: "first".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        let two = second
            .append(
                1,
                Command::StringSet {
                    key: "second".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        let three = first
            .append(
                1,
                Command::StringSet {
                    key: "third".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();

        assert_eq!(one.sequence, 1);
        assert_eq!(two.sequence, 2);
        assert_eq!(three.sequence, 3);
        assert_eq!(last_wal_sequence_at(dir.path(), 1).unwrap(), 3);
    }

    #[test]
    fn wal_full_reclaim_retains_tail_so_sequence_does_not_regress_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for i in 0..5 {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{i}"),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap();
        }
        assert_eq!(store.stats(1).last_sequence, 5);
        // Full reclaim: retain floor past the max sequence would empty the file.
        store.gc_before_sequence(1, 6).unwrap();
        drop(store);
        // Restart: the sequence generator is seeded from the file. The tail record must have
        // survived so the next sequence is 6, not a regressed 1 (which would reuse a sequence
        // <= the durable anchor and be dropped by replay).
        let restarted = LocalWriteAheadLogStore::new(dir.path());
        let next = restarted
            .append(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(
            next.sequence, 6,
            "sequence must continue at 6 after a full reclaim + restart, not regress"
        );
    }

    #[test]
    fn wal_interior_corruption_is_fatal_not_silent_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for i in 0..4 {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{i}"),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap();
        }
        drop(store);
        // Corrupt the 2nd record IN PLACE, keeping it newline-terminated and leaving records
        // 3 & 4 intact after it. A newline-terminated line that fails to parse is committed
        // corruption, not a torn tail.
        let path = write_ahead_log_path(dir.path(), 1);
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 4);
        let corrupted = format!(
            "{}\ncorrupt-not-json\n{}\n{}\n",
            lines[0], lines[2], lines[3]
        );
        std::fs::write(&path, corrupted).unwrap();
        // scan drives last_wal_sequence_at, which must surface the interior corruption as an
        // error rather than silently truncating away records 3 & 4 (which would defeat the
        // strict replay-continuity DataLoss guard).
        let restarted = LocalWriteAheadLogStore::new(dir.path());
        assert!(
            restarted.scan(1, 0, u64::MAX, u64::MAX).is_err(),
            "interior WAL corruption must be fatal, not silently truncated to the last good record"
        );
    }

    // rust-internal: verifies Rust WAL alias exports remain wired to the local mutation log API.
    #[test]
    fn wal_aliases_cover_local_mutation_log_api() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWalStore::new(dir.path());
        let record: WalRecord = store
            .append(
                5,
                Command::StringSet {
                    key: "wal-key".to_string(),
                    value: b"wal-value".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(record.sequence, 1);
        let stats: WalStats = store.stats(5);
        assert_eq!(stats.last_sequence, 1);
        let gc: WalGcReport = store.gc_before_sequence(5, 1).unwrap();
        assert_eq!(gc.records_removed, 0);
    }

    #[test]
    fn gc_before_sequence_rewrites_wal_with_retained_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for key in ["a", "b", "c"] {
            store
                .append(
                    7,
                    Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    },
                )
                .unwrap();
        }

        let report = store.gc_before_sequence(7, 3).unwrap();
        assert_eq!(report.records_before, 3);
        assert_eq!(report.records_after, 1);
        assert_eq!(report.records_removed, 2);
        assert_eq!(store.stats(7).last_sequence, 3);
        let reopened = LocalWriteAheadLogStore::new(dir.path());
        assert_eq!(reopened.stats(7).last_sequence, 3);
        assert_eq!(reopened.scan(7, 0, u64::MAX, u64::MAX).unwrap().len(), 1);
        store
            .append(
                7,
                Command::StringSet {
                    key: "d".to_string(),
                    value: b"d".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(store.stats(7).last_sequence, 4);
    }

    #[test]
    fn corrupt_tail_is_truncated_and_append_resumes_after_last_valid_wal_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append(
                7,
                Command::StringSet {
                    key: "a".to_string(),
                    value: b"a".to_vec(),
                },
            )
            .unwrap();
        store
            .append(
                7,
                Command::StringSet {
                    key: "b".to_string(),
                    value: b"b".to_vec(),
                },
            )
            .unwrap();
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(write_ahead_log_path(dir.path(), 7))
                .unwrap();
            file.write_all(b"{\"shard_id\":7,\"sequence\":3").unwrap();
            file.sync_all().unwrap();
        }

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        assert_eq!(reopened.stats(7).last_sequence, 2);
        assert_eq!(reopened.scan(7, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
        let record = reopened
            .append(
                7,
                Command::StringSet {
                    key: "c".to_string(),
                    value: b"c".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(record.sequence, 3);
        assert_eq!(reopened.scan(7, 0, u64::MAX, u64::MAX).unwrap().len(), 3);
    }

    // shared-corpus: storage_wal_structure_api_flush_parity
    #[test]
    fn wal_record_metadata_tracks_style_log_item_shape() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let record = store
            .append(
                11,
                Command::StringSetEx {
                    key: "profile:7".to_string(),
                    value: b"alice".to_vec(),
                    ttl_ms: 30_000,
                },
            )
            .unwrap();

        let metadata = record.metadata.expect("metadata");
        assert_eq!(metadata.version, WRITE_AHEAD_LOG_FORMAT_VERSION);
        assert_eq!(metadata.items.len(), 1);
        let item = &metadata.items[0];
        assert_eq!(item.item_kind, WriteAheadLogItemKind::Kv);
        assert_eq!(item.model, WriteAheadLogModel::String);
        assert_eq!(item.object_key.as_deref(), Some("profile:7"));
        assert_eq!(item.ttl_ms, Some(30_000));
        assert!(item.bucket_id.is_some());
        assert!(!item.deleted);
        assert!(!item.meta_log);
    }

    // shared-corpus: storage_wal_structure_api_flush_parity
    #[test]
    fn append_replayed_record_is_idempotent_and_flushes_like_stream_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let command = Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        };
        let replayed = WriteAheadLogRecord {
            shard_id: 3,
            sequence: 8,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
            command,
        };

        let first = store.append_replayed_record(replayed.clone()).unwrap();
        assert!(first.appended);
        assert_eq!(first.current_sequence, 8);
        assert!(first.size > 0);
        assert!(first.persistent_bytes >= first.size);
        let duplicate = store.append_replayed_record(replayed).unwrap();
        assert!(!duplicate.appended);
        assert_eq!(duplicate.current_sequence, 8);
        assert_eq!(store.scan(3, 0, u64::MAX, u64::MAX).unwrap().len(), 1);
        let stats = store.stats(3);
        assert_eq!(stats.last_sequence, 8);
        assert_eq!(stats.last_flushed_sequence, 8);
        assert!(stats.flushes >= 1);
        assert!(stats.syncs >= 1);
    }

    // shared-corpus: storage_wal_structure_api_flush_parity
    #[test]
    fn flush_and_info_report_persistent_wal_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append(
                4,
                Command::SetAdd {
                    key: "s".to_string(),
                    member: b"m1".to_vec(),
                },
            )
            .unwrap();
        store
            .append(
                4,
                Command::SetAdd {
                    key: "s".to_string(),
                    member: b"m2".to_vec(),
                },
            )
            .unwrap();

        let flush = store.flush(4).unwrap();
        assert!(flush.synced);
        assert_eq!(flush.last_sequence, 2);
        assert!(flush.persistent_bytes > 0);
        let info = store.info(4).unwrap();
        assert_eq!(info.start_sequence, 1);
        assert_eq!(info.current_sequence, 2);
        assert_eq!(info.records, 2);
        assert_eq!(info.persistent_length_bytes, flush.persistent_bytes);
        assert_eq!(info.format_version, WRITE_AHEAD_LOG_FORMAT_VERSION);
    }
}
