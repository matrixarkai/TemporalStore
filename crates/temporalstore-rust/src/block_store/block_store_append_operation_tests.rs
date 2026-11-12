// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
#[cfg(unix)]
fn singleton_batches_reuse_the_active_append_handle() {
    use std::os::fd::AsRawFd;

    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let first = store
        .append_batch_with_block_metadata(vec![(b"first", Some(7), Some(3), 9)])
        .unwrap();
    let descriptor = store
        .inner
        .lock()
        .unwrap()
        .active_append_file
        .as_ref()
        .unwrap()
        .as_raw_fd();
    let second = store
        .append_batch_with_block_metadata(vec![(b"second", Some(8), Some(4), 10)])
        .unwrap();
    assert_eq!(
        store
            .inner
            .lock()
            .unwrap()
            .active_append_file
            .as_ref()
            .unwrap()
            .as_raw_fd(),
        descriptor
    );
    assert_eq!(store.read(&first[0]).unwrap(), b"first");
    assert_eq!(store.read(&second[0]).unwrap(), b"second");
    assert_eq!(second[0].offset(), first[0].offset() + first[0].length());
    let inner = store.inner.lock().unwrap();
    assert_eq!(inner.stats.writes, 2);
    assert_eq!(inner.stats.logical_bytes_written, 11);
}

#[test]
fn mixed_append_shapes_preserve_bytes_ordinals_rolls_and_reopen() {
    let _target = crate::storage_config::slab_target_scope_for_test(1024);
    let dir = tempfile::tempdir().unwrap();
    let options = BlockStoreOptions {
        compression_enabled: false,
        ..BlockStoreOptions::default()
    };
    let store = BlockStore::with_options(dir.path().join("batch"), options);
    let sequential = BlockStore::with_options(dir.path().join("single"), options);
    let payloads = [
        vec![1; 600],
        vec![2; 600],
        vec![3; 80],
        vec![4; 700],
        vec![5; 100],
    ];
    let records = |index: usize| (payloads[index].as_slice(), Some(42), Some(7), index as u32);
    let mut addresses = store
        .append_batch_with_block_metadata(vec![records(0)])
        .unwrap();
    addresses.extend(
        store
            .append_batch_with_block_metadata(vec![records(1), records(2)])
            .unwrap(),
    );
    addresses.extend(
        store
            .append_batch_with_block_metadata(vec![records(3)])
            .unwrap(),
    );
    addresses.push(
        store
            .append_block_of_object(&payloads[4], Some(42), Some(7), 4)
            .unwrap(),
    );
    let expected: Vec<_> = (0..payloads.len())
        .map(|index| {
            sequential
                .append_block_of_object(&payloads[index], Some(42), Some(7), index as u32)
                .unwrap()
        })
        .collect();
    assert_eq!(addresses, expected);
    assert!(addresses
        .windows(2)
        .any(|pair| pair[0].block_slab_id() != pair[1].block_slab_id()));
    for (address, payload) in addresses.iter().zip(&payloads) {
        assert_eq!(store.read(address).unwrap(), *payload);
        assert_eq!(
            store.read(address).unwrap(),
            sequential.read(address).unwrap()
        );
    }
    store.sync_durable().unwrap();
    drop(store);
    let reopened = BlockStore::with_options(dir.path().join("batch"), options);
    for (address, payload) in addresses.iter().zip(&payloads) {
        assert_eq!(reopened.read(address).unwrap(), *payload);
    }
}

#[test]
fn failed_directory_creation_can_be_retried_without_phantom_writes() {
    for singleton in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("blocks");
        std::fs::write(&root, b"not a directory").unwrap();
        let store = BlockStore::new(&root);
        let count = if singleton { 1 } else { 2 };
        let records = || vec![(b"record".as_slice(), Some(1), Some(2), 3); count];
        assert!(store.append_batch_with_block_metadata(records()).is_err());
        {
            let inner = store.inner.lock().unwrap();
            assert!(!inner.root_created);
            assert_eq!(inner.stats.writes, 0);
        }
        std::fs::remove_file(&root).unwrap();
        let addresses = store.append_batch_with_block_metadata(records()).unwrap();
        assert_eq!(addresses.len(), count);
        for address in addresses {
            assert_eq!(store.read(&address).unwrap(), b"record");
        }
        assert_eq!(store.inner.lock().unwrap().stats.writes, count as u64);
    }
}
