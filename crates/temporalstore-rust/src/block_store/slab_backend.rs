// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Where slab bytes actually live.
//!
//! The block store is the one log in this tree that writes straight to files. The write-ahead
//! log, the index log and the raft log all go through `log_framing`, and the replicator goes
//! through the `ObjectStore` backends -- so the two halves of a storage layer exist here, and
//! the block store uses neither.
//!
//! This is the half the block store was missing: the operations it performs on a slab, named
//! once, so a slab can live on a local file or on an object backend without the block store
//! knowing which. It is deliberately NOT the object-store trait: that one is key-and-bytes with
//! ranges, while a slab is appended to and read back by offset, and mapping one onto the other
//! at every call site is what this exists to avoid.

use std::io;
use std::path::{Path, PathBuf};

/// One slab's worth of storage, addressed by the id the block store already uses.
pub(crate) trait SlabBackend: Send + Sync {
    /// Append bytes to a slab, answering the offset they landed at.
    ///
    /// The offset is the backend's to report rather than the caller's to assume: an object
    /// backend appends into an object whose length it knows, and a caller tracking its own
    /// offset would be guessing at it.
    fn append(&self, slab_id: u64, bytes: &[u8]) -> io::Result<u64>;

    /// Read one record's bytes back, given where the address says they are.
    fn read_range(&self, slab_id: u64, offset: u64, length: u64) -> io::Result<Vec<u8>>;

    /// The whole slab, for the walks that summarise or inspect one.
    fn read_all(&self, slab_id: u64) -> io::Result<Vec<u8>>;

    /// How long the slab is, without reading it.
    fn len(&self, slab_id: u64) -> io::Result<u64>;

    /// Cut a slab back to a length, which is how a torn tail is fenced on reopen.
    fn truncate(&self, slab_id: u64, length: u64) -> io::Result<()>;

    /// Forget a slab entirely.
    fn remove(&self, slab_id: u64) -> io::Result<()>;

    /// Which slabs exist.
    fn slab_ids(&self) -> io::Result<Vec<u64>>;

    /// Make everything written to a slab durable.
    fn sync(&self, slab_id: u64) -> io::Result<()>;

    /// Whether a slab exists at all.
    fn exists(&self, slab_id: u64) -> bool;
}

/// Slabs as files in a directory, which is what this has always done.
///
/// Every method is the code the block store ran inline before, moved behind the name of the
/// operation it performs.
pub(crate) struct LocalSlabBackend<'a> {
    root: &'a Path,
}

impl<'a> LocalSlabBackend<'a> {
    /// Borrows the root rather than owning it, so a caller on a read path can make one without
    /// allocating: the block store already holds the root it would have cloned.
    pub(crate) fn new(root: &'a Path) -> Self {
        Self { root }
    }

    pub(crate) fn root(&self) -> &Path {
        self.root
    }

    fn path(&self, slab_id: u64) -> PathBuf {
        super::slab_path(self.root, slab_id)
    }
}

impl SlabBackend for LocalSlabBackend<'_> {
    fn append(&self, slab_id: u64, bytes: &[u8]) -> io::Result<u64> {
        use std::io::Write as _;
        std::fs::create_dir_all(self.root)?;
        let path = self.path(slab_id);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        // The offset is read from the file rather than tracked by the caller, so a slab that
        // grew underneath us is not silently written over.
        let offset = file.metadata()?.len();
        file.write_all(bytes)?;
        file.flush()?;
        Ok(offset)
    }

    fn read_range(&self, slab_id: u64, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        use std::io::{Read as _, Seek as _, SeekFrom};
        let mut file = std::fs::File::open(self.path(slab_id))?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0_u8; length as usize];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn read_all(&self, slab_id: u64) -> io::Result<Vec<u8>> {
        std::fs::read(self.path(slab_id))
    }

    fn len(&self, slab_id: u64) -> io::Result<u64> {
        Ok(std::fs::metadata(self.path(slab_id))?.len())
    }

    fn truncate(&self, slab_id: u64, length: u64) -> io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.path(slab_id))?;
        file.set_len(length)
    }

    fn remove(&self, slab_id: u64) -> io::Result<()> {
        std::fs::remove_file(self.path(slab_id))
    }

    fn slab_ids(&self) -> io::Result<Vec<u64>> {
        super::slab_ids::slab_ids_at(self.root).map_err(|err| io::Error::other(err.to_string()))
    }

    fn sync(&self, slab_id: u64) -> io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(self.path(slab_id))?;
        file.sync_all()
    }

    fn exists(&self, slab_id: u64) -> bool {
        self.path(slab_id).exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Append answers where the bytes landed, and a read at that offset gives them back.
    #[test]
    fn a_slab_appends_and_reads_back_by_offset() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalSlabBackend::new(dir.path());

        let first = backend.append(0, b"first-record").expect("append");
        let second = backend.append(0, b"second-record").expect("append");
        assert_eq!(first, 0, "the first record starts at the beginning");
        assert_eq!(
            second,
            b"first-record".len() as u64,
            "the second starts where the first ended"
        );
        assert_eq!(
            backend
                .read_range(0, second, b"second-record".len() as u64)
                .expect("read"),
            b"second-record",
        );
        assert_eq!(
            backend.len(0).expect("len"),
            (b"first-record".len() + b"second-record".len()) as u64
        );
    }

    /// Truncating fences a tail, and what was before it still reads.
    #[test]
    fn truncating_keeps_the_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalSlabBackend::new(dir.path());
        backend.append(1, b"keep").expect("append");
        backend.append(1, b"lose").expect("append");
        backend.truncate(1, 4).expect("truncate");
        assert_eq!(backend.len(1).expect("len"), 4);
        assert_eq!(backend.read_all(1).expect("read"), b"keep");
    }

    /// Slabs are found by id, and a removed one is gone.
    #[test]
    fn slabs_are_listed_and_removed_by_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalSlabBackend::new(dir.path());
        backend.append(3, b"three").expect("append");
        backend.append(7, b"seven").expect("append");
        let mut ids = backend.slab_ids().expect("list");
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 7]);
        assert!(backend.exists(3));
        backend.remove(3).expect("remove");
        assert!(!backend.exists(3));
        let ids = backend.slab_ids().expect("list");
        assert_eq!(ids, vec![7]);
    }
}
