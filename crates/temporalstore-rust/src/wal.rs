use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
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
    pub slot_id: Option<u64>,
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
        let slot_id = object_key.as_deref().map(command_slot_id);
        let item_kind = command_item_kind(command);
        Self {
            item_kind,
            model: command_model(command),
            object_key,
            slot_id,
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
    Ips,
    Risk,
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
    pub legacy_path_active: bool,
    pub start_sequence: u64,
    pub current_sequence: u64,
    pub records: usize,
    pub length_bytes: u64,
    pub persistent_length_bytes: u64,
    pub last_flushed_sequence: u64,
    pub format_version: u32,
}

pub const WRITE_AHEAD_LOG_FORMAT_VERSION: u32 = 1;
const WRITE_AHEAD_LOG_BATCH_RECORD_ESTIMATE_BYTES: usize = 192;

#[derive(Debug, Clone)]
pub struct LocalWriteAheadLogStore {
    inner: Arc<Mutex<WriteAheadLogInner>>,
}

#[allow(deprecated)]
pub type WalError = OplogError;
#[allow(deprecated)]
pub type WalRecord = OplogRecord;
#[allow(deprecated)]
pub type WalStats = OplogStats;
#[allow(deprecated)]
pub type WalGcReport = OplogGcReport;
#[allow(deprecated)]
pub type LocalWalStore = LocalOplogStore;

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
        self.append_with_sync(shard_id, command, true)
    }

    pub fn append_with_sync(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_wal_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        let next_sequence = last_sequence.saturating_add(1);
        let record = WriteAheadLogRecord {
            shard_id,
            sequence: next_sequence,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
            command,
        };
        let report = append_record_locked(&mut inner, &record, sync)?;
        inner.stats.last_sequence = report.current_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(record)
    }

    pub fn append_batch_with_sync(
        &self,
        shard_id: ShardId,
        commands: impl IntoIterator<Item = Command>,
        sync: bool,
    ) -> Result<Vec<WriteAheadLogRecord>, WriteAheadLogError> {
        let commands = commands.into_iter().collect::<Vec<_>>();
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        if commands.len() == 1 {
            let command = commands
                .into_iter()
                .next()
                .expect("one-command WAL batch has exactly one command");
            return self
                .append_with_sync(shard_id, command, sync)
                .map(|record| vec![record]);
        }

        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_wal_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        let path = write_ahead_log_path(&inner.root, shard_id);
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let mut records = Vec::with_capacity(commands.len());
        let mut bytes_written = 0_u64;
        let mut batch_bytes = Vec::with_capacity(
            commands
                .len()
                .saturating_mul(WRITE_AHEAD_LOG_BATCH_RECORD_ESTIMATE_BYTES),
        );

        for (index, command) in commands.into_iter().enumerate() {
            let sequence = last_sequence.saturating_add(index as u64).saturating_add(1);
            let record = WriteAheadLogRecord {
                shard_id,
                sequence,
                metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
                command,
            };
            let before_len = batch_bytes.len();
            serde_json::to_writer(&mut batch_bytes, &record)?;
            batch_bytes.push(b'\n');
            bytes_written =
                bytes_written.saturating_add(batch_bytes.len().saturating_sub(before_len) as u64);
            records.push(record);
        }
        file.write_all(&batch_bytes)?;

        if sync {
            file.flush()?;
            file.sync_data()?;
            sync_parent_dir(&path)?;
            inner.stats.flushes += 1;
            inner.stats.syncs += 1;
            if let Some(last) = records.last() {
                inner.stats.last_flushed_sequence = last.sequence;
            }
        }
        let persistent_bytes = file.metadata()?.len();
        if let Some(last) = records.last() {
            inner.stats.last_sequence = last.sequence;
            inner.last_sequence_by_shard.insert(shard_id, last.sequence);
        }
        inner.stats.writes = inner.stats.writes.saturating_add(records.len() as u64);
        inner.stats.bytes_written = inner.stats.bytes_written.saturating_add(bytes_written);
        inner.stats.persistent_bytes = persistent_bytes;
        Ok(records)
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
            let persistent_bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            return Ok(WriteAheadLogAppendReport {
                shard_id: record.shard_id,
                requested_sequence: record.sequence,
                current_sequence: last_sequence,
                appended: false,
                offset: persistent_bytes,
                size: 0,
                persistent_bytes,
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
        if size == 0 {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.stats.reads = inner.stats.reads.saturating_add(1);
            return Ok(Vec::new());
        }
        let root = {
            let inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.root.clone()
        };
        let path = write_ahead_log_path(&root, shard_id);
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if offset >= file_len {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.stats.reads = inner.stats.reads.saturating_add(1);
            return Ok(Vec::new());
        }
        let read_size = size.min(file_len.saturating_sub(offset));
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; read_size as usize];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.stats.reads = inner.stats.reads.saturating_add(1);
        inner.stats.bytes_read = inner.stats.bytes_read.saturating_add(read as u64);
        Ok(bytes)
    }

    pub fn scan(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, WriteAheadLogError> {
        if max_bytes == 0 || start_offset >= end_offset {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.stats.scans = inner.stats.scans.saturating_add(1);
            return Ok(Vec::new());
        }
        let root = {
            let inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.root.clone()
        };
        let _ = last_wal_sequence_at(&root, shard_id)?;
        let path = write_ahead_log_path(&root, shard_id);
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
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.stats.scans = inner.stats.scans.saturating_add(1);
        inner.stats.bytes_read = inner.stats.bytes_read.saturating_add(total);
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
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
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
        let _ = last_wal_sequence_at(&inner.root, shard_id)?;
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
            if record.sequence >= retain_from_sequence {
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
        inner.stats.bytes_written = bytes_after;
        inner.stats.persistent_bytes = bytes_after;
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
        let legacy_path_active = path == legacy_oplog_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(WriteAheadLogInfo {
                shard_id,
                path,
                legacy_path_active,
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
            legacy_path_active,
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
    let wal_path = root.join(format!("shard-{shard_id}.wal.jsonl"));
    let legacy_path = legacy_oplog_path(root, shard_id);
    if legacy_path.exists() && !wal_path.exists() {
        legacy_path
    } else {
        wal_path
    }
}

fn legacy_oplog_path(root: &Path, shard_id: ShardId) -> PathBuf {
    root.join(format!("shard-{shard_id}.oplog.jsonl"))
}

fn append_record_locked(
    inner: &mut WriteAheadLogInner,
    record: &WriteAheadLogRecord,
    sync: bool,
) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
    let path = write_ahead_log_path(&inner.root, record.shard_id);
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    let offset = file.metadata()?.len();
    serde_json::to_writer(&mut file, record)?;
    file.write_all(b"\n")?;
    if sync {
        file.flush()?;
        file.sync_data()?;
        sync_parent_dir(&path)?;
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = record.sequence;
    }
    let persistent_bytes = file.metadata()?.len();
    let bytes_written = persistent_bytes.saturating_sub(offset);
    inner.stats.writes += 1;
    inner.stats.bytes_written += bytes_written;
    inner.stats.persistent_bytes = persistent_bytes;
    Ok(WriteAheadLogAppendReport {
        shard_id: record.shard_id,
        requested_sequence: record.sequence,
        current_sequence: record.sequence,
        appended: true,
        offset,
        size: bytes_written,
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
        let Ok(record) = serde_json::from_slice::<WriteAheadLogRecord>(&line) else {
            break;
        };
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

fn command_slot_id(key: &str) -> u64 {
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
    } else if name.starts_with("ips_") {
        WriteAheadLogModel::Ips
    } else if name.starts_with("risk_") {
        WriteAheadLogModel::Risk
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

#[deprecated(
    since = "0.1.0",
    note = "use WriteAheadLogError; oplog naming remains only for legacy compatibility"
)]
pub type OplogError = WriteAheadLogError;

#[deprecated(
    since = "0.1.0",
    note = "use WriteAheadLogRecord; oplog naming remains only for legacy compatibility"
)]
pub type OplogRecord = WriteAheadLogRecord;

#[deprecated(
    since = "0.1.0",
    note = "use WriteAheadLogStats; oplog naming remains only for legacy compatibility"
)]
pub type OplogStats = WriteAheadLogStats;

#[deprecated(
    since = "0.1.0",
    note = "use WriteAheadLogGcReport; oplog naming remains only for legacy compatibility"
)]
pub type OplogGcReport = WriteAheadLogGcReport;

#[deprecated(
    since = "0.1.0",
    note = "use LocalWriteAheadLogStore; oplog naming remains only for legacy compatibility"
)]
pub type LocalOplogStore = LocalWriteAheadLogStore;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;

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

    #[test]
    // rust-internal: validates legacy WAL filename compatibility after the oplog rename
    fn legacy_oplog_file_is_read_before_new_wal_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = legacy_oplog_path(dir.path(), 7);
        let record = WriteAheadLogRecord {
            shard_id: 7,
            sequence: 1,
            metadata: None,
            command: Command::StringSet {
                key: "legacy".to_string(),
                value: b"v".to_vec(),
            },
        };
        {
            let mut file = File::create(&legacy_path).unwrap();
            serde_json::to_writer(&mut file, &record).unwrap();
            file.write_all(b"\n").unwrap();
        }

        let store = LocalWriteAheadLogStore::new(dir.path());
        assert_eq!(store.stats(7).last_sequence, 1);
        assert_eq!(store.scan(7, 0, u64::MAX, u64::MAX).unwrap().len(), 1);
        let appended = store
            .append(
                7,
                Command::StringSet {
                    key: "new".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(appended.sequence, 2);
        assert!(legacy_path.exists());
        assert!(!dir.path().join("shard-7.wal.jsonl").exists());
    }

    // shared-corpus: storage_wal_oplog_structure_api_flush_parity
    #[test]
    fn wal_record_metadata_tracks_cpp_style_log_item_shape() {
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
        assert!(item.slot_id.is_some());
        assert!(!item.deleted);
        assert!(!item.meta_log);
    }

    // shared-corpus: storage_wal_oplog_structure_api_flush_parity
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

    // shared-corpus: storage_wal_oplog_structure_api_flush_parity
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
        assert!(!info.legacy_path_active);
    }
}
