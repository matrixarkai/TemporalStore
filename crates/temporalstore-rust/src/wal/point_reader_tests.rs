// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct RollPolicy(Option<u64>);
impl RollPolicy {
    fn new(bytes: u64) -> Self {
        Self(SEGMENT_BYTES_OVERRIDE.with(|value| value.replace(Some(bytes))))
    }
}
impl Drop for RollPolicy {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(self.0);
    }
}

fn staged(seed: u8) -> Vec<StagedBlock> {
    (0..128)
        .map(|index| StagedBlock {
            object_id: index,
            element_name: None,
            bytes: (0..1024)
                .map(|offset| ((offset + index * 17 + u64::from(seed)) % 251) as u8)
                .collect(),
        })
        .collect()
}

fn append(store: &LocalWriteAheadLogStore, shard: ShardId, seed: u8) -> (u64, u64) {
    let (record, log_id) = store
        .append_batch_as_one_record_at(shard, Vec::new(), staged(seed), false)
        .unwrap();
    (record.sequence, log_id)
}

fn read(
    store: &LocalWriteAheadLogStore,
    shard: ShardId,
    position: (u64, u64),
    index: usize,
    seed: u8,
) {
    let Some(CarriedBlockRead::Block(bytes)) = store
        .read_carried_block_at_log_id(shard, position.1, position.0, index, index as u64, None)
        .unwrap()
    else {
        panic!("expected indexed block")
    };
    assert_eq!(bytes, staged(seed)[index].bytes);
}

#[cfg(unix)]
#[test]
fn sealed_readers_reuse_the_descriptor_without_relisting_or_reopening() {
    let _policy = RollPolicy::new(1);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let position = append(&store, 7, 1);
    FILE_OPENS.with(|count| count.set(0));
    WAL_SEGMENT_LISTINGS.with(|count| count.set(0));
    read(&store, 7, position, 0, 1);
    assert_eq!(FILE_OPENS.with(|count| count.get()), 1);
    assert_eq!(WAL_SEGMENT_LISTINGS.with(|count| count.get()), 1);
    for index in [127, 9, 63, 0, 1] {
        read(&store, 7, position, index, 1);
    }
    assert_eq!(FILE_OPENS.with(|count| count.get()), 1);
    assert_eq!(WAL_SEGMENT_LISTINGS.with(|count| count.get()), 1);
    assert_eq!(store.inner.lock().unwrap().point_readers.readers.len(), 1);
}

#[cfg(unix)]
#[test]
fn a_sealed_descriptor_serves_different_records_inside_its_extent() {
    let _policy = RollPolicy::new(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, 7, 1);
    let second = append(&store, 7, 2);
    set_wal_segment_bytes_for_test(Some(1));
    append(&store, 7, 3);
    FILE_OPENS.with(|count| count.set(0));
    read(&store, 7, first, 127, 1);
    read(&store, 7, second, 127, 2);
    assert_eq!(FILE_OPENS.with(|count| count.get()), 1);
    assert_eq!(store.inner.lock().unwrap().point_readers.readers.len(), 1);
}

#[test]
fn growing_active_segments_do_not_retain_descriptors() {
    let _policy = RollPolicy::new(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, 7, 1);
    read(&store, 7, first, 127, 1);
    let second = append(&store, 7, 2);
    read(&store, 7, second, 127, 2);
    read(&store, 7, first, 0, 1);
    let inner = store.inner.lock().unwrap();
    assert!(inner.point_readers.readers.is_empty());
    assert_eq!(inner.point_readers.open_file_bytes, 0);
}

#[cfg(unix)]
#[test]
fn owned_reclaim_closes_descriptors_before_unlink_and_preserves_survivors() {
    let _policy = RollPolicy::new(1);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, 7, 1);
    let second = append(&store, 7, 2);
    read(&store, 7, first, 0, 1);
    read(&store, 7, second, 0, 2);
    assert_eq!(store.inner.lock().unwrap().point_readers.readers.len(), 2);
    store.gc_before_sequence_unchecked(7, second.0).unwrap();
    {
        let inner = store.inner.lock().unwrap();
        assert!(inner.point_readers.readers.is_empty());
        assert_eq!(inner.point_readers.open_file_bytes, 0);
    }
    assert!(store
        .read_carried_block_at_log_id(7, first.1, first.0, 0, 0, None)
        .unwrap()
        .is_none());
    read(&store, 7, second, 127, 2);
}

#[cfg(unix)]
#[test]
fn replaced_same_length_files_do_not_serve_cached_inode_bytes() {
    let _policy = RollPolicy::new(1);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let position = append(&store, 7, 1);
    read(&store, 7, position, 127, 1);
    let path = sealed_wal_path(dir.path(), 7, position.1);
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    let replacement = path.with_extension("replacement");
    fs::write(&replacement, bytes).unwrap();
    fs::rename(replacement, &path).unwrap();
    assert!(store
        .read_carried_block_at_log_id(7, position.1, position.0, 127, 127, None)
        .is_err());
    assert!(store.inner.lock().unwrap().point_readers.readers.is_empty());
}

#[cfg(unix)]
#[test]
fn deleted_sealed_files_are_not_read_through_retained_descriptors() {
    let _policy = RollPolicy::new(1);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let position = append(&store, 7, 1);
    read(&store, 7, position, 0, 1);
    fs::remove_file(sealed_wal_path(dir.path(), 7, position.1)).unwrap();
    assert!(store
        .read_carried_block_at_log_id(7, position.1, position.0, 0, 0, None)
        .unwrap()
        .is_none());
    let inner = store.inner.lock().unwrap();
    assert!(inner.point_readers.readers.is_empty());
    assert_eq!(inner.point_readers.open_file_bytes, 0);
}

#[cfg(unix)]
#[test]
fn stale_directory_generations_force_one_refresh_when_another_writer_rolls() {
    let _policy = RollPolicy::new(1);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, 7, 1);
    read(&store, 7, first, 0, 1);
    let other = LocalWriteAheadLogStore::new(dir.path());
    let second = append(&other, 7, 2);
    {
        // Simulate a filesystem whose directory timestamps did not distinguish the roll.
        let mut inner = store.inner.lock().unwrap();
        let directory = inner.point_readers.directories.front_mut().unwrap();
        Arc::get_mut(directory).unwrap().stamp = stamp(&dir.path().metadata().unwrap());
    }
    WAL_SEGMENT_LISTINGS.with(|count| count.set(0));
    read(&store, 7, second, 127, 2);
    assert_eq!(WAL_SEGMENT_LISTINGS.with(|count| count.get()), 1);
}

#[cfg(unix)]
#[test]
fn reader_cache_keeps_shards_and_store_owners_separate() {
    let _policy = RollPolicy::new(1);
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let first = LocalWriteAheadLogStore::new(a.path());
    let second = LocalWriteAheadLogStore::new(b.path());
    let first_seven = append(&first, 7, 1);
    let first_eight = append(&first, 8, 2);
    let second_seven = append(&second, 7, 3);
    for _ in 0..3 {
        read(&first, 7, first_seven, 127, 1);
        read(&first, 8, first_eight, 127, 2);
        read(&second, 7, second_seven, 127, 3);
    }
    assert_eq!(first.inner.lock().unwrap().point_readers.readers.len(), 2);
    assert_eq!(second.inner.lock().unwrap().point_readers.readers.len(), 1);
}

#[cfg(unix)]
#[test]
fn descriptor_count_and_retained_metadata_stay_bounded() {
    let _policy = RollPolicy::new(1);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let positions: Vec<_> = (0..MAX_READERS + 8)
        .map(|index| append(&store, 7, index as u8))
        .collect();
    for (index, position) in positions.into_iter().enumerate() {
        read(&store, 7, position, 0, index as u8);
        let inner = store.inner.lock().unwrap();
        let cache = &inner.point_readers;
        assert!(cache.readers.len() <= MAX_READERS);
        assert!(
            cache.reader_bytes + cache.readers.capacity() * std::mem::size_of::<Reader>()
                <= READER_BYTES
        );
        assert!(cache.open_file_bytes <= OPEN_FILE_BYTES);
        assert!(
            cache.directory_bytes
                + cache.directories.capacity() * std::mem::size_of::<Arc<Directory>>()
                <= DIRECTORY_BYTES
        );
    }
}

#[cfg(unix)]
#[test]
fn represented_file_lengths_limit_delayed_external_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = Cache::default();
    let length = 40 * 1024 * 1024u64;
    for index in 0..3 {
        let base = index * length;
        let path = sealed_wal_path(dir.path(), 7, base);
        let mut file = File::create(path).unwrap();
        let header = crate::log_framing::encode_base_header(base);
        file.write_all(&header).unwrap();
        file.write_all(&[crate::log_framing::FRAME_MAGIC_V3, 1, 2, 3])
            .unwrap();
        file.set_len(length + header.len() as u64).unwrap();
        let reader = cache.take(dir.path(), 7, base).unwrap().unwrap();
        cache.put(reader);
        assert_eq!(cache.readers.len(), 1);
        assert!(cache.open_file_bytes <= OPEN_FILE_BYTES);
    }
}
