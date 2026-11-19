// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn location(store: &LocalWriteAheadLogStore, sequence: u64, expected: usize) -> Registration {
    Arc::new((store.clone(), sequence, sequence, expected, Some(0)))
}

#[test]
fn summaries_combine_domains_and_follow_replacement_retirement_and_clear() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let other = LocalWriteAheadLogStore::new(dir.path().join("other"));
    let owner = blocks.store_id();
    let key = (17, (u64::MAX, 1, 8));
    registry()
        .lock()
        .unwrap()
        .shard_mut(owner, 1)
        .insert((18, None), location(&wal, 7, 2));
    assert!(registered_record_span_has_holes(&blocks, 1, &wal));
    let previous = location(&wal, 7, 2);
    install(&blocks, 1, key, previous.clone());
    assert_eq!(min_registered_sequence(&blocks, 1, &wal), Some(7));
    assert!(!registered_record_span_has_holes(&blocks, 1, &wal));
    let current = location(&wal, 8, 2);
    install(&blocks, 1, key, current.clone());
    registry()
        .lock()
        .unwrap()
        .shard_mut(owner, 1)
        .insert((18, None), location(&wal, 8, 2));
    assert_eq!(min_registered_sequence(&blocks, 1, &wal), Some(8));
    assert!(!registered_record_span_has_holes(&blocks, 1, &wal));
    retire(
        &blocks,
        1,
        &RegistrationRetirement {
            folded_key: folded(key),
            log_id: 7,
            sequence: 7,
            block: RetirementKey::Physical(key),
            location: previous,
        },
    );
    assert_eq!(min_registered_sequence(&blocks, 1, &wal), Some(8));
    install(&blocks, 1, key, location(&other, 3, 1));
    assert!(registered_record_span_has_holes(&blocks, 1, &wal));
    assert_eq!(min_registered_sequence(&blocks, 1, &other), Some(3));
    assert!(!registered_record_span_has_holes(&blocks, 1, &other));
    crate::engine::block_in_wal::clear_shard(&blocks, 1);
    assert_eq!(min_registered_sequence(&blocks, 1, &wal), None);
    assert_eq!(min_registered_sequence(&blocks, 1, &other), None);
    assert!(!registered_record_span_has_holes(&blocks, 1, &wal));
}

#[test]
fn summary_queries_complete_while_both_locator_registries_are_locked() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    install_many(
        &blocks,
        1,
        (1..=1000).map(|sequence| {
            (
                (sequence, (u64::MAX, sequence, 8)),
                location(&wal, sequence, 1),
            )
        }),
    );
    let logical = registry().lock().unwrap();
    let physical = table().lock().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = {
        let blocks = blocks.clone();
        let wal = wal.clone();
        std::thread::spawn(move || {
            for _ in 0..100 {
                assert_eq!(min_registered_sequence(&blocks, 1, &wal), Some(1));
                assert!(!registered_record_span_has_holes(&blocks, 1, &wal));
            }
            send.send(()).unwrap();
        })
    };
    let result = receive.recv_timeout(std::time::Duration::from_secs(5));
    drop(physical);
    drop(logical);
    reader.join().unwrap();
    assert!(
        result.is_ok(),
        "summary reads must not wait for locator registry scans"
    );
}
