// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Default)]
struct Facts {
    anchor: u64,
    wal_floor: Option<u64>,
}

#[cfg(test)]
thread_local! {
    static DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn facts(path: &Path) -> Facts {
    let Ok(bytes) = std::fs::read(path) else {
        return Facts::default();
    };
    // Same-size writes can share every stat timestamp. Validate the actual bytes on each
    // lookup, and retain only scalar facts rather than a shard-sized decoded image.
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    static CACHE: OnceLock<Mutex<Vec<(PathBuf, [u8; 32], Facts)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(Vec::new()));
    let cached = cache
        .lock()
        .expect("base anchor cache poisoned")
        .iter()
        .find(|(p, hash, _)| p == path && *hash == digest)
        .map(|(_, _, facts)| *facts);
    if let Some(facts) = cached {
        return facts;
    }
    #[cfg(test)]
    DECODES.with(|count| count.set(count.get() + 1));
    let Ok(image) = super::decode_index_bytes(&bytes) else {
        return Facts::default();
    };
    let facts = Facts {
        anchor: image.applied_wal_sequence.unwrap_or(0),
        wal_floor: super::storage_lifecycle_methods::wal_floor_of_index_image(&image),
    };
    let mut entries = cache.lock().expect("base anchor cache poisoned");
    entries.retain(|(p, _, _)| p != path);
    if entries.len() == 32 {
        entries.remove(0);
    }
    entries.push((path.to_path_buf(), digest, facts));
    facts
}

pub(super) fn read(path: &Path) -> u64 {
    facts(path).anchor
}

pub(super) fn wal_floor(path: &Path) -> Option<u64> {
    facts(path).wal_floor
}

#[cfg(test)]
mod tests {
    use super::*;
    fn image(anchor: u64) -> Vec<u8> {
        let mut shard = super::super::ShardState::default();
        shard.index_format_version = super::super::SHARD_INDEX_FORMAT_VERSION;
        shard.applied_wal_sequence = Some(anchor);
        super::super::encode_index_bytes(&shard)
    }

    #[test]
    fn replacement_and_older_overwrite_invalidate_the_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("base");
        std::fs::write(&path, image(99)).unwrap();
        assert_eq!(read(&path), 99);
        assert_eq!(read(&path), 99);
        let next = dir.path().join("next");
        std::fs::write(&next, image(41)).unwrap();
        std::fs::rename(next, &path).unwrap();
        assert_eq!(read(&path), 41);
        std::fs::write(&path, image(12)).unwrap();
        assert_eq!(read(&path), 12);
    }

    #[test]
    fn absent_corrupt_and_recreated_images_are_revalidated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("base");
        assert_eq!(read(&path), 0);
        std::fs::write(&path, image(73)).unwrap();
        assert_eq!(read(&path), 73);
        std::fs::write(&path, b"not an index").unwrap();
        assert_eq!(read(&path), 0);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(read(&path), 0);
        std::fs::write(&path, image(21)).unwrap();
        assert_eq!(read(&path), 21);
    }

    #[test]
    fn cached_facts_preserve_live_unknown_and_absent_wal_placements() {
        use super::super::{Command, ExecuteRequest, TemporalEngine};
        let _on = crate::engine::value_once_in_test(true);
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "key".into(),
                        value: b"value".to_vec()
                    },
                })
                .status
                .ok
        );
        let mut shard = engine.shards.with_state(1, |state| state.unwrap().clone());
        let expected = super::super::storage_lifecycle_methods::wal_floor_of_index_image(&shard);
        assert!(expected.is_some_and(|floor| floor > 0));
        let path = dir.path().join("base");
        let before = DECODES.with(|count| count.get());
        std::fs::write(&path, super::super::encode_index_bytes(&shard)).unwrap();
        assert_eq!(wal_floor(&path), expected);
        assert_eq!(read(&path), shard.applied_wal_sequence.unwrap_or(0));
        assert_eq!(wal_floor(&path), expected);
        assert_eq!(DECODES.with(|count| count.get()) - before, 1);

        shard.wal_resident_blocks.clear();
        std::fs::write(&path, super::super::encode_index_bytes(&shard)).unwrap();
        assert_eq!(wal_floor(&path), Some(0));
        assert_eq!(DECODES.with(|count| count.get()) - before, 2);
        std::fs::write(&path, image(12)).unwrap();
        assert_eq!(wal_floor(&path), None);
        assert_eq!(read(&path), 12);
        assert_eq!(DECODES.with(|count| count.get()) - before, 3);
    }
}
