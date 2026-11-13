// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded, identity-checked file reuse for carried-block reads. No payload is cached here.

use super::*;
use std::collections::VecDeque;

const MAX_READERS: usize = 128;
const READER_BYTES: usize = 256 * 1024;
const OPEN_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_DIRECTORIES: usize = 8;
const DIRECTORY_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stamp {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    length: u64,
    #[cfg(unix)]
    modified: (i64, i64),
    #[cfg(unix)]
    changed: (i64, i64),
}

fn stamp(metadata: &fs::Metadata) -> Option<Stamp> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(Stamp {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

#[derive(Debug)]
pub(super) struct Reader {
    shard: ShardId,
    base: u64,
    header: u64,
    length: u64,
    sealed: bool,
    path: PathBuf,
    stamp: Option<Stamp>,
    pub(super) file: File,
    pub(super) physical: u64,
}

struct Directory {
    shard: ShardId,
    stamp: Option<Stamp>,
    pieces: Vec<WalPiece>,
    weight: usize,
}

impl std::fmt::Debug for Directory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Directory")
            .field("shard", &self.shard)
            .field("pieces", &self.pieces.len())
            .field("weight", &self.weight)
            .finish()
    }
}

#[derive(Debug, Default)]
pub(super) struct Cache {
    readers: VecDeque<Reader>,
    reader_bytes: usize,
    open_file_bytes: u64,
    directories: VecDeque<Arc<Directory>>,
    directory_bytes: usize,
}

impl Cache {
    pub(super) fn take(
        &mut self,
        root: &Path,
        shard: ShardId,
        log_id: u64,
    ) -> Result<Option<Reader>, WriteAheadLogError> {
        if let Some(index) = self.readers.iter().position(|reader| {
            reader.shard == shard
                && log_id >= reader.base
                && log_id - reader.base < reader.length.saturating_sub(reader.header)
        }) {
            let mut reader = self.readers.remove(index).unwrap();
            self.reader_bytes -= reader.path.capacity();
            self.open_file_bytes -= reader.length;
            match fs::metadata(&reader.path) {
                Ok(metadata) if reader.stamp.is_some() && stamp(&metadata) == reader.stamp => {
                    reader.physical = reader.header + (log_id - reader.base);
                    return Ok(Some(reader));
                }
                Ok(_) => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
        // A directory timestamp can have coarse granularity, and another process may roll
        // between listing and opening. A failed lookup refreshes once even when its stamp matches.
        for force in [false, true] {
            let directory = self.directory(root, shard, force)?;
            let sealed = directory.pieces.len().saturating_sub(1);
            let upper = directory.pieces[..sealed]
                .partition_point(|piece| piece.starts_at.is_some_and(|base| base <= log_id));
            let first = upper.saturating_sub(1);
            for piece in &directory.pieces[first..] {
                if piece.starts_at.is_some_and(|base| base > log_id) {
                    break;
                }
                let mut file = match self.open(&piece.path) {
                    Ok(file) => file,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.into()),
                };
                #[cfg(test)]
                FILE_OPENS.with(|count| count.set(count.get() + 1));
                let (base, header) = read_wal_base_from_file(&mut file)?;
                let metadata = file.metadata()?;
                if log_id < base {
                    break;
                }
                if log_id - base < metadata.len().saturating_sub(header) {
                    return Ok(Some(Reader {
                        shard,
                        base,
                        header,
                        length: metadata.len(),
                        sealed: piece.starts_at.is_some(),
                        path: piece.path.clone(),
                        stamp: stamp(&metadata),
                        file,
                        physical: header + (log_id - base),
                    }));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn put(&mut self, reader: Reader) {
        if let Some(index) = self
            .readers
            .iter()
            .position(|old| old.shard == reader.shard && old.path == reader.path)
        {
            let old = self.readers.remove(index).unwrap();
            self.reader_bytes -= old.path.capacity();
            self.open_file_bytes -= old.length;
        }
        // Active pieces grow and can be replaced by reclaim. Keep their reads uncached;
        // retained descriptors only name immutable sealed extents with a bounded total size.
        if !reader.sealed
            || reader.stamp.is_none()
            || reader.path.capacity() > READER_BYTES
            || reader.length > OPEN_FILE_BYTES
        {
            return;
        }
        if self.readers.len() >= MAX_READERS {
            self.retire_oldest();
        }
        self.readers.reserve(1);
        let queue_bytes = self.readers.capacity() * std::mem::size_of::<Reader>();
        while self.reader_bytes + reader.path.capacity() + queue_bytes > READER_BYTES
            || self.open_file_bytes + reader.length > OPEN_FILE_BYTES
        {
            if !self.retire_oldest() {
                return;
            }
        }
        self.reader_bytes += reader.path.capacity();
        self.open_file_bytes += reader.length;
        self.readers.push_back(reader);
    }

    pub(super) fn clear_shard(&mut self, shard: ShardId) {
        self.readers.retain(|reader| reader.shard != shard);
        self.reader_bytes = self
            .readers
            .iter()
            .map(|reader| reader.path.capacity())
            .sum();
        self.open_file_bytes = self.readers.iter().map(|reader| reader.length).sum();
        self.directories
            .retain(|directory| directory.shard != shard);
        self.directory_bytes = self
            .directories
            .iter()
            .map(|directory| directory.weight)
            .sum();
    }

    pub(super) fn clear_directory(&mut self, shard: ShardId) {
        self.directories
            .retain(|directory| directory.shard != shard);
        self.directory_bytes = self
            .directories
            .iter()
            .map(|directory| directory.weight)
            .sum();
    }

    fn retire_oldest(&mut self) -> bool {
        let Some(old) = self.readers.pop_front() else {
            return false;
        };
        self.reader_bytes -= old.path.capacity();
        self.open_file_bytes -= old.length;
        true
    }

    fn open(&mut self, path: &Path) -> std::io::Result<File> {
        match File::open(path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound && self.retire_oldest() => {
                File::open(path)
            }
            result => result,
        }
    }

    fn directory(
        &mut self,
        root: &Path,
        shard: ShardId,
        force: bool,
    ) -> Result<Arc<Directory>, WriteAheadLogError> {
        let current = match root.metadata() {
            Ok(metadata) => stamp(&metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(index) = self
            .directories
            .iter()
            .position(|entry| entry.shard == shard)
        {
            let directory = self.directories.remove(index).unwrap();
            self.directory_bytes -= directory.weight;
            if !force && current.is_some() && directory.stamp == current {
                self.directory_bytes += directory.weight;
                self.directories.push_back(Arc::clone(&directory));
                return Ok(directory);
            }
        }
        let entries = match fs::read_dir(root) {
            Ok(entries) => Some(entries),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) if self.retire_oldest() => Some(fs::read_dir(root)?),
            Err(error) => return Err(error.into()),
        };
        let pieces =
            wal_segment_pieces_from_entries(root, shard, entries.into_iter().flatten().flatten());
        let weight = std::mem::size_of::<Directory>()
            + 2 * std::mem::size_of::<usize>()
            + pieces.capacity() * std::mem::size_of::<WalPiece>()
            + pieces
                .iter()
                .map(|piece| piece.path.capacity())
                .sum::<usize>();
        let directory = Arc::new(Directory {
            shard,
            stamp: current,
            pieces,
            weight,
        });
        if current.is_none() || weight > DIRECTORY_BYTES {
            return Ok(directory);
        }
        if self.directories.len() >= MAX_DIRECTORIES {
            if let Some(old) = self.directories.pop_front() {
                self.directory_bytes -= old.weight;
            }
        }
        self.directories.reserve(1);
        let queue_bytes = self.directories.capacity() * std::mem::size_of::<Arc<Directory>>();
        while self.directory_bytes + weight + queue_bytes > DIRECTORY_BYTES {
            let Some(old) = self.directories.pop_front() else {
                return Ok(directory);
            };
            self.directory_bytes -= old.weight;
        }
        self.directory_bytes += weight;
        self.directories.push_back(Arc::clone(&directory));
        Ok(directory)
    }
}

#[cfg(test)]
thread_local! {
    static FILE_OPENS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
#[path = "point_reader_tests.rs"]
mod tests;
