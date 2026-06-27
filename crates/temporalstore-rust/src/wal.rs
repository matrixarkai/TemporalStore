use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub last_sequence: u64,
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

#[derive(Debug, Clone)]
pub struct LocalWriteAheadLogStore {
    inner: Arc<Mutex<WriteAheadLogInner>>,
}

pub type WalError = OplogError;
pub type WalRecord = OplogRecord;
pub type WalStats = OplogStats;
pub type WalGcReport = OplogGcReport;
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
            command,
        };
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(write_ahead_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        file.sync_data()?;
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(record)
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
        WriteAheadLogStats {
            last_sequence: last_wal_sequence_at(&inner.root, shard_id).unwrap_or_default(),
            ..inner.stats
        }
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
}
