// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::hash::BuildHasher;

#[test]
fn registration_hashing_keeps_full_field_identity_and_replacements() {
    let dir = tempfile::tempdir().unwrap();
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let first = Arc::new((wal.clone(), 1, 1, 1, Some(0)));
    let next = Arc::new((wal, 2, 2, 1, Some(1)));
    let long = format!("{}-\u{96ea}", "feature-name-".repeat(256));
    let names = [None, Some(""), Some("field"), Some(long.as_str())];
    let mut map = ShardRegistrations::default();
    for object in [0, 1, u64::MAX] {
        for name in names {
            assert!(map.insert(block_key(object, name), first.clone()).is_none());
        }
    }
    assert_eq!(map.len(), 12);
    for object in [0, 1, u64::MAX] {
        for name in names {
            // Construct fresh field allocations so equality cannot depend on Arc identity.
            let key = block_key(object, name);
            assert!(Arc::ptr_eq(map.get(&key).unwrap(), &first));
            assert!(Arc::ptr_eq(&map.insert(key.clone(), next.clone()).unwrap(), &first));
            assert!(Arc::ptr_eq(map.get(&key).unwrap(), &next));
        }
    }
    assert_eq!(map.len(), 12);
    for name in names {
        assert!(map.remove(&block_key(1, name)).is_some());
        assert!(map.get(&block_key(1, name)).is_none());
        assert!(map.get(&block_key(0, name)).is_some());
        assert!(map.get(&block_key(u64::MAX, name)).is_some());
    }
    assert_eq!(map.len(), 8);
}

fn replace_and_read<S: BuildHasher>(
    map: &mut HashMap<BlockKey, Registration, S>,
    keys: &[BlockKey],
    replacement: &Registration,
    rounds: usize,
) -> u128 {
    let started = std::time::Instant::now();
    for _ in 0..rounds {
        for key in keys {
            std::hint::black_box(map.insert(key.clone(), replacement.clone()));
        }
        for key in keys {
            std::hint::black_box(map.get(std::hint::black_box(key)).unwrap());
        }
    }
    started.elapsed().as_nanos()
}

#[test]
#[ignore = "release timing fixture"]
fn record_registration_hashing_cost() {
    let dir = tempfile::tempdir().unwrap();
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    let location = Arc::new((wal, 1, 1, 1, Some(0)));
    for field_bytes in [12, 256] {
        let fields: Vec<Arc<str>> = (0..20)
            .map(|field| Arc::from(format!("field-{field:02}-{}", "x".repeat(field_bytes - 9))))
            .collect();
        let keys: Vec<_> = (0..500).flat_map(|object| {
            fields.iter().map(move |field| (object, Some(field.clone())))
        }).collect();
        let mut standard = HashMap::new();
        let mut selected = ShardRegistrations::default();
        for key in &keys {
            standard.insert(key.clone(), location.clone());
            selected.insert(key.clone(), location.clone());
        }
        assert_eq!(standard.capacity(), selected.capacity());
        for sample in 0..4 {
            let rounds = 32;
            let (standard_ns, selected_ns) = if sample % 2 == 0 {
                (replace_and_read(&mut standard, &keys, &location, rounds),
                 replace_and_read(&mut selected, &keys, &location, rounds))
            } else {
                let selected_ns = replace_and_read(&mut selected, &keys, &location, rounds);
                (replace_and_read(&mut standard, &keys, &location, rounds), selected_ns)
            };
            assert_eq!(standard.len(), selected.len());
            assert!(keys.iter().all(|key| Arc::ptr_eq(standard.get(key).unwrap(), selected.get(key).unwrap())));
            eprintln!("REGISTRATION_HASH field_bytes={field_bytes} sample={sample} entries={} capacity={} replacements={} reads={} standard_ns={standard_ns} selected_ns={selected_ns}",
                keys.len(), selected.capacity(), keys.len() * rounds, keys.len() * rounds);
        }
    }
}
