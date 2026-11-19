// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded, identity-checked file reuse for carried-block reads. No payload is cached here.

use super::*;
use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};

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

const CURRENT_SPELLING: usize = 0;
const LEGACY_SPELLING: usize = 1;
const FIRST_ALIAS: usize = 2;

struct Piece {
    start: u64,
    spelling: usize,
}

struct NameWriter<'a>(&'a mut OsString);

impl std::fmt::Write for NameWriter<'_> {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0.push(text);
        Ok(())
    }
}

struct Directory {
    shard: ShardId,
    stamp: Option<Stamp>,
    pieces: Vec<Piece>,
    aliases: Vec<OsString>,
    names: SealedNameParts,
    active: PathBuf,
    weight: usize,
}

fn canonical_spelling(name: &OsStr, parts: &SealedNameParts) -> Option<usize> {
    let tail = name.to_str()?.strip_prefix(parts.prefix.as_str())?;
    let (digits, spelling) = tail
        .strip_suffix(parts.suffix.as_str())
        .map(|digits| (digits, CURRENT_SPELLING))
        .or_else(|| {
            tail.strip_suffix(parts.legacy_suffix.as_str())
                .map(|digits| (digits, LEGACY_SPELLING))
        })?;
    (digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(spelling)
}

impl Directory {
    fn from_entries(
        root: &Path,
        shard: ShardId,
        stamp: Option<Stamp>,
        entries: impl Iterator<Item = fs::DirEntry>,
    ) -> Self {
        #[cfg(test)]
        WAL_SEGMENT_LISTINGS.with(|count| count.set(count.get() + 1));
        let names = SealedNameParts::for_shard(shard);
        let mut pieces = Vec::new();
        let mut aliases = Vec::new();
        for entry in entries {
            #[cfg(test)]
            WAL_SEGMENT_LISTING_ENTRIES.with(|count| count.set(count.get() + 1));
            let name = entry.file_name();
            let Some(start) = sealed_wal_start_log_id_with(Path::new(&name), &names) else {
                continue;
            };
            let spelling = canonical_spelling(&name, &names).unwrap_or_else(|| {
                let spelling = FIRST_ALIAS + aliases.len();
                aliases.push(name);
                spelling
            });
            pieces.push(Piece { start, spelling });
        }
        pieces.sort_by_key(|piece| piece.start);
        // Retaining spare slots can make an otherwise bounded directory miss admission.
        pieces.shrink_to_fit();
        let active = write_ahead_log_path(root, shard);
        let weight = std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + pieces.capacity() * std::mem::size_of::<Piece>()
            + aliases.capacity() * std::mem::size_of::<OsString>()
            + aliases.iter().map(|name| name.capacity()).sum::<usize>()
            + names.prefix.capacity()
            + names.suffix.capacity()
            + names.legacy_suffix.capacity()
            + active.capacity();
        Self {
            shard,
            stamp,
            pieces,
            aliases,
            names,
            active,
            weight,
        }
    }

    fn path(&self, root: &Path, index: usize) -> PathBuf {
        let Some(piece) = self.pieces.get(index) else {
            return self.active.clone();
        };
        match piece.spelling {
            CURRENT_SPELLING | LEGACY_SPELLING => {
                let suffix = if piece.spelling == CURRENT_SPELLING {
                    &self.names.suffix
                } else {
                    &self.names.legacy_suffix
                };
                let mut path = PathBuf::with_capacity(
                    root.as_os_str().len() + 1 + self.names.prefix.len() + 20 + suffix.len(),
                );
                path.push(root);
                path.push(&self.names.prefix);
                // Format into the path allocation without a separate filename buffer.
                std::fmt::write(
                    &mut NameWriter(path.as_mut_os_string()),
                    format_args!("{:020}{}", piece.start, suffix),
                ).expect("formatting a log file name");
                path
            }
            alias => {
                let name = &self.aliases[alias - FIRST_ALIAS];
                let mut path = PathBuf::with_capacity(root.as_os_str().len() + 1 + name.len());
                path.push(root);
                path.push(name);
                path
            }
        }
    }
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
            let sealed = directory.pieces.len();
            let upper = directory.pieces
                .partition_point(|piece| piece.start <= log_id);
            let first = upper.saturating_sub(1);
            for index in first..=sealed {
                if directory.pieces.get(index).is_some_and(|piece| piece.start > log_id) {
                    break;
                }
                let path = directory.path(root, index);
                let mut file = match self.open(&path) {
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
                        sealed: index < sealed,
                        path,
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
        let directory = Arc::new(Directory::from_entries(
            root, shard, current, entries.into_iter().flatten().flatten(),
        ));
        let weight = directory.weight;
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
