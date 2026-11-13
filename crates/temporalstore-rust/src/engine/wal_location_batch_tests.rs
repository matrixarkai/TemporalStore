// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn empty_location_batch_does_not_create_shard_state() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    assert!(install_many(&blocks, 1, std::iter::empty()).is_empty());
    assert!(!table().lock().unwrap().contains_key(&blocks.store_id()));
}

#[test]
fn batched_locations_preserve_replacements_and_scope() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let other = crate::block_store::BlockStore::new(dir.path().join("other"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let keys: Vec<_> = (0..128).map(|i| (42, (3, i * 8, 8))).collect();
    let old = Arc::new((wal.clone(), 1, 1, 128, Some(0)));
    let current = Arc::new((wal, 2, 2, 128, Some(1)));
    let expected: Vec<_> = keys.iter().copied().map(folded).collect();
    assert_eq!(
        install_many(&blocks, 1, keys.iter().map(|key| (*key, old.clone()))),
        expected
    );
    install_many(&blocks, 2, keys.iter().map(|key| (*key, old.clone())));
    install_many(&other, 1, keys.iter().map(|key| (*key, old.clone())));
    assert_eq!(
        install_many(&blocks, 1, keys.iter().map(|key| (*key, current.clone()))),
        expected
    );
    assert_eq!(count(&blocks, 1), 128);
    assert_eq!(count(&blocks, 2), 128);
    assert_eq!(count(&other, 1), 128);
    for key in keys {
        assert!(Arc::ptr_eq(
            &lookup(&blocks, 1, key.0, key.1).unwrap().unwrap(),
            &current
        ));
        assert!(Arc::ptr_eq(
            &lookup(&blocks, 2, key.0, key.1).unwrap().unwrap(),
            &old
        ));
        assert!(Arc::ptr_eq(
            &lookup(&other, 1, key.0, key.1).unwrap().unwrap(),
            &old
        ));
    }
    assert!(matches!(lookup(&blocks, 1, 42, (3, 9999, 8)), Some(None)));
}

#[test]
fn mixed_record_keeps_logical_names_and_exact_physical_positions() {
    use crate::block_store::ElementEntry;
    use crate::wal::WalOutcomeItem;

    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let mut staged = Vec::new();
    let mut outcomes = Vec::new();
    for at in 0..128 {
        let name = format!("{at}");
        staged.push(StagedBlock {
            object_id: 42,
            element_name: Some(name.as_str().into()),
            bytes: vec![at as u8; 8],
        });
        outcomes.push(WalOutcomeItem {
            kind: "feature".into(),
            object_key: "series".into(),
            element_name: Some(name),
            object_id: 42,
            routing_bucket: 0,
            address: Some(ElementEntry::from_parts(
                crate::wal_record::WAL_LOG_SLAB_ID,
                at * 8,
                8,
                None,
                None,
            )),
            value: None,
            ttl: None,
            deleted: false,
            meta: false,
        });
    }
    let hash_bytes =
        crate::engine::container_blocks::encode_utf8_container_block(&[("left", b"value")]);
    outcomes.push(WalOutcomeItem {
        kind: "hash".into(),
        object_key: "row".into(),
        element_name: None,
        object_id: 99,
        routing_bucket: 0,
        address: Some(ElementEntry::from_parts(
            crate::wal_record::WAL_LOG_SLAB_ID,
            4096,
            hash_bytes.len() as u64,
            None,
            None,
        )),
        value: None,
        ttl: None,
        deleted: false,
        meta: false,
    });
    staged.push(StagedBlock {
        object_id: 99,
        element_name: None,
        bytes: hash_bytes,
    });
    staged.push(StagedBlock {
        object_id: 777,
        element_name: None,
        bytes: b"unbound".to_vec(),
    });
    let keys = register_record_with_outcomes(&blocks, 1, &staged, &outcomes, 7, 19, &wal);
    assert_eq!(keys.len(), 130);
    assert_eq!(count(&blocks, 1), 128);
    assert_eq!(registration_count(&blocks, 1), 130);
    assert!(keys.contains(&wal_resident_key(99, Some("left"))));
    assert!(keys.contains(&wal_resident_key(777, None)));
    for index in 0..128 {
        let key = (
            42,
            (crate::wal_record::WAL_LOG_SLAB_ID, index as u64 * 8, 8),
        );
        let location = lookup(&blocks, 1, key.0, key.1).unwrap().unwrap();
        assert!(keys.contains(&folded(key)));
        assert_eq!(
            (location.1, location.2, location.3, location.4),
            (7, 19, 130, Some(index))
        );
    }
    let registry = registry().lock().unwrap();
    let names = registry.shard(blocks.store_id(), 1).unwrap();
    assert_eq!(names.len(), 2);
    assert_eq!(names[&block_key(99, Some("left"))].4, Some(128));
    assert_eq!(names[&block_key(777, None)].4, Some(129));
}
