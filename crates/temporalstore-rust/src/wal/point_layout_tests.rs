// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use super::super::{pack, PointReader, HEADER_BYTES};
use std::io::{Read, Seek, SeekFrom, Write};

fn source() -> (tempfile::NamedTempFile, Vec<u8>) {
    let payload: Vec<u8> = (0..1024 * 1024).map(|index| (index % 251) as u8).collect();
    let encoded = pack(&payload, 0, 0, 11);
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&encoded).unwrap();
    (file, payload)
}

#[test]
#[cfg(unix)]
fn validated_layout_reuses_footer_work_without_retaining_payload() {
    let (file, payload) = source();
    let mut opened = file.reopen().unwrap();
    let version = super::super::super::point_readers::stamp(&opened.metadata().unwrap());
    let cache = Cache::default();
    let cold = PointReader::with_layout_cache(&mut opened, 0, version, &cache)
        .unwrap().unwrap().physical_bytes;
    assert!(cold > 1024);
    for offset in [0, 4090, 500_000, payload.len() - 100] {
        let mut reader = PointReader::with_layout_cache(&mut opened, 0, version, &cache)
            .unwrap().unwrap();
        assert_eq!(reader.physical_bytes, HEADER_BYTES as u64);
        reader.seek(SeekFrom::Start(offset as u64)).unwrap();
        let mut selected = [0; 100];
        reader.read_exact(&mut selected).unwrap();
        assert_eq!(selected, payload[offset..offset + 100]);
        assert_eq!(reader.physical_bytes, (HEADER_BYTES + selected.len()) as u64);
    }
    let state = cache.state.lock().unwrap();
    assert_eq!(state.entries.len(), 1);
    assert!(state.weight() < 4096);
    println!("cold_transport_bytes={cold} warm_transport_bytes={} metadata_bytes={}", HEADER_BYTES, state.weight());
}

#[test]
#[cfg(unix)]
fn changed_file_version_revalidates_corrupt_footer_and_truncation() {
    let (mut file, _) = source();
    let cache = Cache::default();
    let mut opened = file.reopen().unwrap();
    let version = super::super::super::point_readers::stamp(&opened.metadata().unwrap());
    PointReader::with_layout_cache(&mut opened, 0, version, &cache).unwrap().unwrap();
    let encoded = std::fs::read(file.path()).unwrap();
    let first = u32::from_le_bytes(encoded[9..13].try_into().unwrap()) as usize;
    file.seek(SeekFrom::Start((HEADER_BYTES + first) as u64)).unwrap();
    file.write_all(&[encoded[HEADER_BYTES + first] ^ 1]).unwrap();
    // Changing length as well makes this independent of filesystem clock resolution.
    file.as_file().set_len(encoded.len() as u64 + 1).unwrap();
    let changed = super::super::super::point_readers::stamp(&opened.metadata().unwrap());
    assert_ne!(changed, version);
    assert!(PointReader::with_layout_cache(&mut opened, 0, changed, &cache).is_err());
    file.as_file().set_len((HEADER_BYTES + first) as u64).unwrap();
    let truncated = super::super::super::point_readers::stamp(&opened.metadata().unwrap());
    assert!(PointReader::with_layout_cache(&mut opened, 0, truncated, &cache).is_err());
    assert_eq!(cache.state.lock().unwrap().entries.len(), 1);
}

#[test]
#[cfg(unix)]
fn layout_cache_bounds_entries_and_separates_file_identity_and_offsets() {
    let (file, _) = source();
    let cache = Cache::default();
    let version = super::super::super::point_readers::stamp(&file.as_file().metadata().unwrap()).unwrap();
    for offset in 0..10_000 {
        cache.insert(version, offset, 8192, 4000);
        let state = cache.state.lock().unwrap();
        assert!(state.entries.len() <= MAX_ENTRIES);
        assert_eq!(state.entries.len(), state.oldest.len());
        assert!(state.weight() <= MAX_BYTES);
    }
    assert!(!cache.contains(version, 0, 8192, 4000));
    assert!(cache.contains(version, 9999, 8192, 4000));
    assert!(!cache.contains(version, 9999, 8193, 4000));
    let mut other = tempfile::NamedTempFile::new().unwrap();
    other.write_all(&std::fs::read(file.path()).unwrap()).unwrap();
    let other_version = super::super::super::point_readers::stamp(&other.as_file().metadata().unwrap()).unwrap();
    assert_ne!(version, other_version);
    assert!(!cache.contains(other_version, 9999, 8192, 4000));
    assert!(!Cache::default().contains(version, 9999, 8192, 4000));
}

#[test]
fn unstamped_point_reads_keep_validating_the_transport() {
    let (file, _) = source();
    let cache = Cache::default();
    let mut opened = file.reopen().unwrap();
    let cold = PointReader::with_layout_cache(&mut opened, 0, None, &cache)
        .unwrap().unwrap().physical_bytes;
    let again = PointReader::with_layout_cache(&mut opened, 0, None, &cache)
        .unwrap().unwrap().physical_bytes;
    assert_eq!(cold, again);
    assert!(cold > HEADER_BYTES as u64);
    assert!(cache.state.lock().unwrap().entries.is_empty());
}
