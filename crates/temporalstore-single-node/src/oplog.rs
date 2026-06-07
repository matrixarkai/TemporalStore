use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{Command, ShardId};

#[derive(Debug, Error)]
pub enum OplogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OplogRecord {
    pub shard_id: ShardId,
    pub sequence: u64,
    pub command: Command,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OplogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub last_sequence: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OplogGcReport {
    pub shard_id: ShardId,
    pub retain_from_sequence: u64,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

#[derive(Debug, Clone)]
pub struct LocalOplogStore {
    inner: Arc<Mutex<OplogInner>>,
}

#[derive(Debug)]
struct OplogInner {
    root: PathBuf,
    stats: OplogStats,
}

impl LocalOplogStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(OplogInner {
                root,
                stats: OplogStats::default(),
            })),
        }
    }

    pub fn append(&self, shard_id: ShardId, command: Command) -> Result<OplogRecord, OplogError> {
        let mut inner = self.inner.lock().expect("oplog lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let next_sequence = last_sequence_at(&inner.root, shard_id)?.saturating_add(1);
        let record = OplogRecord {
            shard_id,
            sequence: next_sequence,
            command,
        };
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(oplog_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        Ok(record)
    }

    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OplogError> {
        let mut inner = self.inner.lock().expect("oplog lock poisoned");
        let path = oplog_path(&inner.root, shard_id);
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
    ) -> Result<Vec<(u64, Vec<u8>)>, OplogError> {
        let mut inner = self.inner.lock().expect("oplog lock poisoned");
        let path = oplog_path(&inner.root, shard_id);
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
    ) -> Result<OplogGcReport, OplogError> {
        let inner = self.inner.lock().expect("oplog lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = oplog_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(OplogGcReport {
                shard_id,
                retain_from_sequence,
                ..OplogGcReport::default()
            });
        }

        let bytes_before = path.metadata()?.len();
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
            let record: OplogRecord = serde_json::from_str(&line)?;
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
        }
        fs::rename(&temp_path, &path)?;
        let bytes_after = path.metadata()?.len();
        Ok(OplogGcReport {
            shard_id,
            retain_from_sequence,
            records_before,
            records_after: retained.len(),
            records_removed: records_before.saturating_sub(retained.len()),
            bytes_before,
            bytes_after,
        })
    }

    pub fn stats(&self, shard_id: ShardId) -> OplogStats {
        let inner = self.inner.lock().expect("oplog lock poisoned");
        OplogStats {
            last_sequence: last_sequence_at(&inner.root, shard_id).unwrap_or_default(),
            ..inner.stats
        }
    }
}

impl Default for LocalOplogStore {
    fn default() -> Self {
        Self::new(unique_temp_path("oplogs"))
    }
}

fn oplog_path(root: &std::path::Path, shard_id: ShardId) -> PathBuf {
    root.join(format!("shard-{shard_id}.oplog.jsonl"))
}

fn last_sequence_at(root: &std::path::Path, shard_id: ShardId) -> Result<u64, OplogError> {
    let path = oplog_path(root, shard_id);
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut last = 0;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: OplogRecord = serde_json::from_str(&line)?;
        last = last.max(record.sequence);
    }
    Ok(last)
}

fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-single-node-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;

    #[test]
    fn gc_before_sequence_rewrites_oplog_with_retained_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalOplogStore::new(dir.path());
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
}
