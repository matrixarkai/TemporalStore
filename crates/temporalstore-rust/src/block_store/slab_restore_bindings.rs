// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::io::{self, ErrorKind};
use std::path::Path;

use super::record::{encode_slab_owner, slab_owner, OWNED_SLAB_HEADER_LEN};
use super::{make_block_address_word, BlockAddress, ElementEntry, MAX_ADDRESSABLE_BLOCK_SLAB_ID};
use super::{BlockStoreError, BlockStoreInner};

const MAGIC: &[u8; 8] = b"TSBIND02";
const MAX_IDENTITY_BYTES: usize = 4096;
const WATERMARK_FORMAT: &[u8; 8] = b"TSRANGE2";
const WATERMARK_NAME: &str = "slab-restore-watermark";

pub(super) fn read_restore_watermark(root: &Path) -> io::Result<Option<(u64, u64)>> {
    let bytes = match std::fs::read(root.join(WATERMARK_NAME)) {
        Ok(bytes) => bytes,
        Err(error) if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let (body, checksum) = bytes
        .split_last_chunk::<4>()
        .ok_or_else(|| invalid("truncated slab restore watermark"))?;
    let range = body
        .strip_prefix(WATERMARK_FORMAT)
        .filter(|range| range.len() == 16)
        .ok_or_else(|| invalid("invalid slab restore watermark"))?;
    let high = u64::from_le_bytes(range[..8].try_into().unwrap());
    let legacy_cursor = u64::from_le_bytes(range[8..].try_into().unwrap());
    if high > MAX_ADDRESSABLE_BLOCK_SLAB_ID
        || legacy_cursor >= high
        || crate::checksum::crc32c(body) != u32::from_le_bytes(*checksum)
    {
        return Err(invalid("invalid slab restore watermark"));
    }
    Ok(Some((high, legacy_cursor)))
}

pub(super) fn persist_restore_watermark(
    root: &Path,
    high: u64,
    legacy_cursor: u64,
) -> io::Result<()> {
    let mut bytes = [0u8; 28];
    bytes[..8].copy_from_slice(WATERMARK_FORMAT);
    bytes[8..16].copy_from_slice(&high.to_le_bytes());
    bytes[16..24].copy_from_slice(&legacy_cursor.to_le_bytes());
    let checksum = crate::checksum::crc32c(&bytes[..24]);
    bytes[24..].copy_from_slice(&checksum.to_le_bytes());
    persist_restore_metadata(&root.join(WATERMARK_NAME), &bytes)
}

fn persist_restore_metadata(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(temp, path)?;
    super::sync_parent_dir(path)
}

fn valid_generation(generation: &str) -> bool {
    generation.len() == 64 && generation.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn persist_lazy_manifest(root: &Path, generation: &str, bytes: &[u8]) -> io::Result<()> {
    if !valid_generation(generation) {
        return Err(invalid("invalid checkpoint generation fingerprint"));
    }
    let path = root.join(format!("lazy-checkpoint-{generation}.manifest"));
    match std::fs::read(&path) {
        Ok(existing) if existing == bytes => Ok(()),
        Ok(_) => Err(invalid("saved lazy checkpoint manifest changed")),
        Err(error) if error.kind() == ErrorKind::NotFound => persist_restore_metadata(&path, bytes),
        Err(error) => Err(error),
    }
}

pub(super) fn read_lazy_manifests(root: &Path) -> io::Result<Vec<(String, Vec<u8>)>> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut manifests = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(generation) = name
            .to_str()
            .and_then(|name| name.strip_prefix("lazy-checkpoint-"))
            .and_then(|name| name.strip_suffix(".manifest"))
        else {
            continue;
        };
        if !valid_generation(generation) {
            return Err(invalid("invalid saved lazy checkpoint name"));
        }
        manifests.push((generation.to_owned(), std::fs::read(entry.path())?));
    }
    Ok(manifests)
}

pub(super) fn has_lazy_manifests(root: &Path) -> io::Result<bool> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let name = entry?.file_name();
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("lazy-checkpoint-") && name.ends_with(".manifest"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn load_or_bind_checkpoint(
    inner: &mut BlockStoreInner,
    generation: &str,
    source_ids: &[u32],
    allow_reserve: bool,
) -> Result<SlabRestoreBindings, BlockStoreError> {
    inner.check_owned_recovery()?;
    if !valid_generation(generation) {
        return Err(invalid("invalid checkpoint generation fingerprint").into());
    }
    let max = MAX_ADDRESSABLE_BLOCK_SLAB_ID as u32;
    let source_ids = source_ids.iter().copied().collect::<BTreeSet<_>>();
    if source_ids.last().is_some_and(|&id| id > max) {
        return Err(invalid("invalid source slab ID").into());
    }
    if source_ids.is_empty() {
        return Ok(SlabRestoreBindings::default());
    }
    let path = inner
        .root
        .join(format!("slab-restore-{generation}.bindings"));
    match std::fs::read(&path) {
        Ok(bytes) => {
            let bindings = SlabRestoreBindings::decode(&bytes, max)?;
            let valid = bindings.by_source.len() == 1
                && bindings.by_source.get(generation).is_some_and(|slabs| {
                    slabs.keys().copied().eq(source_ids.iter().copied())
                        && slabs
                            .values()
                            .all(|&id| u64::from(id) <= inner.owned_highest_id)
                });
            if !valid {
                return Err(invalid("stored bindings disagree with checkpoint generation").into());
            }
            Ok(bindings)
        }
        Err(error) if error.kind() == ErrorKind::NotFound && allow_reserve => {
            let reserved = inner.reserve_checkpoint_slab_ids(source_ids.len() as u64)?;
            let mut bindings = SlabRestoreBindings::default();
            for (slab_id, local_id) in source_ids.into_iter().zip(reserved) {
                bindings.bind(
                    SourceSlab {
                        lineage: generation.to_owned(),
                        slab_id,
                    },
                    local_id as u32,
                    max,
                )?;
            }
            persist_restore_metadata(&path, &bindings.encode())?;
            Ok(bindings)
        }
        Err(error) => Err(error.into()),
    }
}

/// Exact durable identity of a source allocation lineage. This value must come
/// from the producer's durable metadata, rather than from a shard number alone.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SourceSlab {
    pub lineage: String,
    pub slab_id: u32,
}

/// A restore plan owns no allocator state. Register only IDs reserved by the
/// destination allocator. Persist the encoded plan inside the checked recovery
/// transaction before publishing any address that uses it.
#[derive(Debug, Default)]
pub struct SlabRestoreBindings {
    by_source: BTreeMap<String, BTreeMap<u32, u32>>,
    destination_ids: BTreeSet<u32>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

impl SlabRestoreBindings {
    pub(crate) fn translate_entry(
        &self,
        lineage: &str,
        entry: &mut ElementEntry,
    ) -> io::Result<()> {
        let local_id = self.translate_slab_id(lineage, entry.block_slab_id())?;
        entry.address =
            BlockAddress::from_word(make_block_address_word(local_id, entry.offset() as u32));
        Ok(())
    }

    pub(crate) fn translate_slab_id(&self, lineage: &str, source_id: u64) -> io::Result<u32> {
        if crate::wal_record::is_wal_resident(source_id) {
            return Err(invalid("synthetic address requires WAL translation"));
        }
        let source_id = u32::try_from(source_id).map_err(|_| invalid("invalid source slab ID"))?;
        self.resolve_physical(lineage, source_id)
    }

    pub fn bind(&mut self, source: SourceSlab, local_id: u32, max_id: u32) -> io::Result<()> {
        let max_id = max_id.min(MAX_ADDRESSABLE_BLOCK_SLAB_ID as u32);
        if source.lineage.is_empty()
            || source.lineage.len() > MAX_IDENTITY_BYTES
            || source.slab_id > max_id
            || local_id > max_id
        {
            return Err(invalid("invalid physical slab binding"));
        }
        if let Some(previous) = self
            .by_source
            .get(&source.lineage)
            .and_then(|slabs| slabs.get(&source.slab_id))
        {
            if *previous == local_id {
                return Ok(());
            }
            return Err(invalid("source slab already has a different destination"));
        }
        if !self.destination_ids.insert(local_id) {
            return Err(invalid(
                "destination slab already belongs to a different source",
            ));
        }
        self.by_source
            .entry(source.lineage)
            .or_default()
            .insert(source.slab_id, local_id);
        Ok(())
    }

    pub fn resolve(&self, source: &SourceSlab) -> io::Result<u32> {
        self.resolve_physical(&source.lineage, source.slab_id)
    }

    /// Resolve the slab half only. Callers retain the original offset and
    /// block metadata, and handle synthetic WAL addresses before this lookup.
    pub fn resolve_physical(&self, lineage: &str, source_id: u32) -> io::Result<u32> {
        self.by_source
            .get(lineage)
            .and_then(|slabs| slabs.get(&source_id))
            .copied()
            .ok_or_else(|| invalid("missing physical slab binding"))
    }

    /// Retarget already integrity-checked transfer bytes to their reserved local ID.
    /// Record offsets and payload checksums remain valid because the slab header
    /// keeps its size and the records do not encode a physical slab ID.
    pub(crate) fn translate_slab(
        &self,
        source: &SourceSlab,
        bytes: &mut [u8],
        owner: Option<u64>,
    ) -> Result<u32, BlockStoreError> {
        let local = self.resolve(source)?;
        if let Some(owner) = owner {
            if slab_owner(bytes, u64::from(source.slab_id))? != Some(owner) {
                return Err(invalid("slab does not match the declared transfer owner").into());
            }
            bytes[..OWNED_SLAB_HEADER_LEN]
                .copy_from_slice(&encode_slab_owner(u64::from(local), owner));
        }
        Ok(local)
    }

    pub(crate) fn local_sources(&self, lineage: &str) -> io::Result<BTreeMap<u64, u32>> {
        let mappings = self
            .by_source
            .get(lineage)
            .ok_or_else(|| invalid("missing source lineage"))?;
        Ok(mappings
            .iter()
            .map(|(&source, &local)| (u64::from(local), source))
            .collect())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&(self.by_source.len() as u64).to_le_bytes());
        for (lineage, slabs) in &self.by_source {
            bytes.extend_from_slice(&(lineage.len() as u16).to_le_bytes());
            bytes.extend_from_slice(lineage.as_bytes());
            bytes.extend_from_slice(&(slabs.len() as u64).to_le_bytes());
            for (source_id, local_id) in slabs {
                bytes.extend_from_slice(&source_id.to_le_bytes());
                bytes.extend_from_slice(&local_id.to_le_bytes());
            }
        }
        let checksum = crate::checksum::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8], max_id: u32) -> io::Result<Self> {
        let (bytes, checksum) = bytes
            .split_last_chunk::<4>()
            .ok_or_else(|| invalid("missing slab binding checksum"))?;
        if crate::checksum::crc32c(bytes) != u32::from_le_bytes(*checksum) {
            return Err(invalid("invalid slab binding checksum"));
        }
        let mut bytes = bytes
            .strip_prefix(MAGIC)
            .ok_or_else(|| invalid("unsupported slab binding format"))?;
        let count = u64::from_le_bytes(take(&mut bytes)?);
        if count > (bytes.len() / 19) as u64 {
            return Err(invalid("incomplete slab binding table"));
        }
        let mut bindings = Self::default();
        for _ in 0..count {
            let length = u16::from_le_bytes(take(&mut bytes)?) as usize;
            if length == 0 || length > MAX_IDENTITY_BYTES || length > bytes.len() {
                return Err(invalid("invalid source lineage length"));
            }
            let (lineage, remaining) = bytes.split_at(length);
            bytes = remaining;
            let lineage =
                std::str::from_utf8(lineage).map_err(|_| invalid("invalid source lineage"))?;
            if bindings
                .by_source
                .last_key_value()
                .is_some_and(|(previous, _)| previous.as_str() >= lineage)
            {
                return Err(invalid("source lineages must be unique and ordered"));
            }
            let slab_count = u64::from_le_bytes(take(&mut bytes)?);
            if slab_count == 0 || slab_count > (bytes.len() / 8) as u64 {
                return Err(invalid("invalid source slab count"));
            }
            let mut previous_id = None;
            for _ in 0..slab_count {
                let slab_id = u32::from_le_bytes(take(&mut bytes)?);
                let local_id = u32::from_le_bytes(take(&mut bytes)?);
                if previous_id.is_some_and(|previous| previous >= slab_id) {
                    return Err(invalid("source slabs must be unique and ordered"));
                }
                bindings.bind(
                    SourceSlab {
                        lineage: lineage.to_owned(),
                        slab_id,
                    },
                    local_id,
                    max_id,
                )?;
                previous_id = Some(slab_id);
            }
        }
        if !bytes.is_empty() {
            return Err(invalid("trailing bytes in slab binding table"));
        }
        Ok(bindings)
    }
}

fn take<const N: usize>(bytes: &mut &[u8]) -> io::Result<[u8; N]> {
    let prefix = bytes
        .get(..N)
        .ok_or_else(|| invalid("truncated slab binding"))?;
    *bytes = &bytes[N..];
    Ok(prefix.try_into().expect("checked binding field length"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_store::BlockStore;

    const MAX: u32 = u32::MAX - 2;

    #[test]
    fn damaged_restore_watermark_refuses_writes_without_modifying_slab_bytes() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let first = store.append(b"first").unwrap();
        store.sync_durable().unwrap();
        store.reserve_checkpoint_slab_ids(2).unwrap();
        let before = store.read_slab(first.block_slab_id()).unwrap();
        drop(store);
        let path = root.path().join(WATERMARK_NAME);
        let mut damaged = std::fs::read(&path).unwrap();
        *damaged.last_mut().unwrap() ^= 1;
        std::fs::write(&path, &damaged).unwrap();

        let reopened = BlockStore::new(root.path());
        assert!(reopened.append(b"cannot publish").is_err());
        assert!(reopened
            .append_block_of_object_for_shard(0, b"cannot publish", None, None, 0)
            .is_err());
        assert_eq!(reopened.read_slab(first.block_slab_id()).unwrap(), before);
        assert_eq!(std::fs::read(path).unwrap(), damaged);
    }

    #[test]
    fn reserved_destination_ids_survive_a_restart_before_any_import() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let active = store.current_block_slab_id();
        let first = store.reserve_checkpoint_slab_ids(3).unwrap();
        assert_eq!(store.current_block_slab_id(), active);
        assert_eq!(first.clone().count(), 3);
        assert!(store.reserve_checkpoint_slab_ids(0).is_err());
        assert!(store.reserve_checkpoint_slab_ids(u64::MAX).is_err());
        drop(store);

        let store = BlockStore::new(root.path());
        let legacy = store.append(b"legacy").unwrap();
        let owned = store
            .append_block_of_object_for_shard(0, b"owned", None, None, 0)
            .unwrap();
        let second = store.reserve_checkpoint_slab_ids(2).unwrap();
        assert!(legacy.block_slab_id() > *first.end());
        assert!(owned.block_slab_id() > *first.end());
        assert!(*second.start() > owned.block_slab_id());
    }

    fn source(lineage: &str, slab_id: u32) -> SourceSlab {
        SourceSlab {
            lineage: lineage.to_owned(),
            slab_id,
        }
    }

    #[test]
    fn independent_lineages_with_equal_ids_survive_reopen() {
        let first = source("root-a/epoch-1", 1);
        let second = source("root-b/epoch-1", 1);
        let mut map = SlabRestoreBindings::default();
        map.bind(second.clone(), 9, MAX).unwrap();
        map.bind(first.clone(), 8, MAX).unwrap();
        let bytes = map.encode();
        let restored = SlabRestoreBindings::decode(&bytes, MAX).unwrap();
        assert_eq!(restored.resolve(&first).unwrap(), 8);
        assert_eq!(restored.resolve(&second).unwrap(), 9);
        assert_eq!(bytes, restored.encode());
    }

    #[test]
    fn retries_preserve_bindings_and_conflicts_change_nothing() {
        let first = source("root-a", 1);
        let mut map = SlabRestoreBindings::default();
        map.bind(first.clone(), MAX, MAX).unwrap();
        let before = map.encode();
        map.bind(first.clone(), MAX, MAX).unwrap();
        assert!(map.bind(first, 2, MAX).is_err());
        assert!(map.bind(source("root-b", 1), MAX, MAX).is_err());
        assert!(map.bind(source("root-b", MAX + 1), 2, MAX).is_err());
        assert!(map.bind(source("root-b", 1), MAX + 1, MAX).is_err());
        assert!(map.resolve(&source("root-b", 1)).is_err());
        assert_eq!(map.encode(), before);
    }

    #[test]
    fn decode_refuses_partial_duplicate_or_out_of_range_records() {
        let mut map = SlabRestoreBindings::default();
        map.bind(source("root-a", 1), 8, MAX).unwrap();
        let bytes = map.encode();
        for length in 0..bytes.len() {
            assert!(SlabRestoreBindings::decode(&bytes[..length], MAX).is_err());
        }
        let mut duplicate = bytes.clone();
        duplicate.truncate(duplicate.len() - 4);
        let repeated = duplicate[16..].to_vec();
        duplicate[8..16].copy_from_slice(&2u64.to_le_bytes());
        duplicate.extend_from_slice(&repeated);
        duplicate.extend_from_slice(&crate::checksum::crc32c(&duplicate).to_le_bytes());
        assert!(SlabRestoreBindings::decode(&duplicate, MAX).is_err());
        assert!(SlabRestoreBindings::decode(&bytes, 7).is_err());
    }

    #[test]
    fn physical_translation_preserves_non_slab_bits() {
        let mut map = SlabRestoreBindings::default();
        map.bind(source("root-a", 1), 8, MAX).unwrap();
        let mut entry = ElementEntry::try_from_parts(1, 27, 12, Some(3), None).unwrap();
        entry.set_holder_bits(93);
        let mut expected = entry.clone();
        expected.address = BlockAddress::from_word(make_block_address_word(8, 27));
        map.translate_entry("root-a", &mut entry).unwrap();
        assert_eq!(entry, expected);
        assert_eq!(entry.holder_bits(), 93);
        assert!(map.translate_entry("missing", &mut entry).is_err());
        assert_eq!(entry, expected);

        let mut wal =
            ElementEntry::try_from_parts(crate::wal_record::WAL_LOG_SLAB_ID, 27, 12, Some(3), None)
                .unwrap();
        let original = wal.clone();
        assert!(map.translate_entry("root-a", &mut wal).is_err());
        assert_eq!(wal, original);
    }

    #[test]
    fn physical_limit_applies_even_with_an_overlarge_bound() {
        let mut map = SlabRestoreBindings::default();
        for reserved in [MAX + 1, MAX + 2] {
            assert!(map.bind(source("root-a", reserved), 1, u32::MAX).is_err());
            assert!(map.bind(source("root-a", 1), reserved, u32::MAX).is_err());
        }
    }

    #[test]
    fn only_declared_owned_slab_headers_are_translated() {
        let mut map = SlabRestoreBindings::default();
        let source = source("root-a", 1);
        map.bind(source.clone(), 8, MAX).unwrap();
        let mut bytes = encode_slab_owner(1, 42).to_vec();
        bytes.extend_from_slice(b"record bytes");
        let original = bytes.clone();
        assert_eq!(map.translate_slab(&source, &mut bytes, None).unwrap(), 8);
        assert_eq!(bytes, original);
        assert!(map.translate_slab(&source, &mut bytes, Some(7)).is_err());
        assert_eq!(bytes, original);
        assert_eq!(
            map.translate_slab(&source, &mut bytes, Some(42)).unwrap(),
            8
        );
        assert_eq!(slab_owner(&bytes, 8).unwrap(), Some(42));
        assert_eq!(&bytes[OWNED_SLAB_HEADER_LEN..], b"record bytes");
    }

    #[test]
    fn encoded_lineage_cost_is_paid_once() {
        let mut map = SlabRestoreBindings::default();
        let lineage = "a".repeat(MAX_IDENTITY_BYTES);
        for id in 0..128 {
            map.bind(source(&lineage, id), id + 128, MAX).unwrap();
        }
        let bytes = map.encode();
        assert_eq!(bytes.len(), 16 + 2 + lineage.len() + 8 + 128 * 8 + 4);
        assert_eq!(
            SlabRestoreBindings::decode(&bytes, MAX).unwrap().encode(),
            bytes
        );
    }
}
