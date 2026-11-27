// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn absent_empty_and_foreign_quarantine_entries_are_not_slabs() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    assert!(!store.has_delayed_destroy_slabs().unwrap());
    let trash = delayed_destroy_dir(dir.path());
    std::fs::create_dir_all(&trash).unwrap();
    assert!(!store.has_delayed_destroy_slabs().unwrap());
    std::fs::write(trash.join("unrelated"), b"keep").unwrap();
    assert!(!store.has_delayed_destroy_slabs().unwrap());
    assert!(trash.join("unrelated").exists());
}

#[test]
fn an_unrecorded_quarantined_slab_is_seen_without_changing_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let trash = delayed_destroy_dir(dir.path());
    std::fs::create_dir_all(&trash).unwrap();
    let file = trash.join("block_segment_00000000000000000007.seg.deleted.1");
    std::fs::write(&file, b"recoverable").unwrap();
    assert!(!store.slab_descriptors().iter().any(|slab| slab.block_slab_id == 7));
    assert!(store.has_delayed_destroy_slabs().unwrap());
    assert!(store.has_delayed_destroy_slabs().unwrap());
    assert_eq!(std::fs::read(file).unwrap(), b"recoverable");
}

#[test]
fn an_invalid_quarantine_directory_is_an_error_not_an_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    std::fs::write(delayed_destroy_dir(dir.path()), b"not a directory").unwrap();
    assert!(store.has_delayed_destroy_slabs().is_err());
}
