// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn entry() -> LiveBlockEntry {
    LiveBlockEntry {
        object_key: Arc::from("object|with|separators"),
        kind: StoredModelKind::ALL[0],
        element_name: Some(Arc::from("element|name")),
        address: ElementEntry::from_parts(17, 256, 8192, Some(9), None),
        dirty: true,
        deleted: false,
        log_backed: true,
        filed_routing_bucket: 23,
        filing_is_known: true,
    }
}

fn text(shard: ShardId, entry: &LiveBlockEntry, bucket: u32) -> String {
    let mut line = String::new();
    write_bucket_fingerprint_line(&mut line, shard, entry, bucket);
    line
}

#[test]
fn typed_fingerprint_tracks_every_textual_term() {
    let base = entry();
    let mut cases = vec![(1, base.clone(), 23)];
    cases.push((2, base.clone(), 23));
    cases.push((1, base.clone(), 24));
    for &kind in StoredModelKind::ALL {
        let mut changed = base.clone();
        changed.kind = kind;
        cases.push((1, changed, 23));
    }
    for key in ["", "object", "object|with", "object\0with", "object\u{e9}"] {
        let mut changed = base.clone();
        changed.object_key = Arc::from(key);
        cases.push((1, changed, 23));
    }
    for name in [
        None,
        Some(""),
        Some("element"),
        Some("element|"),
        Some("\0"),
    ] {
        let mut changed = base.clone();
        changed.element_name = name.map(Arc::from);
        cases.push((1, changed, 23));
    }
    for (slab, offset, length, block_id, generation) in [
        (18, 256, 8192, Some(9), true),
        (17, 257, 8192, Some(9), true),
        (17, 256, 8193, Some(9), true),
        (17, 256, 8192, Some(10), true),
        (17, 256, 8192, Some(9), false),
        (17, 256, 8192, None, false),
        (17, 256, 8192, Some(0), true),
    ] {
        let mut changed = base.clone();
        changed.address =
            ElementEntry::try_from_wire_parts(slab, offset, length, block_id, generation).unwrap();
        cases.push((1, changed, 23));
    }
    for flags in 0..8 {
        let mut changed = base.clone();
        changed.dirty = flags & 1 != 0;
        changed.deleted = flags & 2 != 0;
        changed.log_backed = flags & 4 != 0;
        cases.push((1, changed, 23));
    }
    for (left_shard, left, left_bucket) in &cases {
        for (right_shard, right, right_bucket) in &cases {
            assert_eq!(
                text(*left_shard, left, *left_bucket) == text(*right_shard, right, *right_bucket),
                fingerprint_fields_hash(*left_shard, left, *left_bucket)
                    == fingerprint_fields_hash(*right_shard, right, *right_bucket),
                "field comparison disagrees: {left:?} vs {right:?}",
            );
        }
    }
}

#[test]
fn typed_fingerprint_preserves_empty_and_zero_aliases() {
    let mut absent = entry();
    absent.element_name = None;
    absent.address = ElementEntry::from_parts(17, 256, 8192, None, None);
    let mut zero = absent.clone();
    zero.element_name = Some(Arc::from(""));
    zero.address = ElementEntry::from_parts(17, 256, 8192, Some(0), None);
    assert_eq!(text(1, &absent, 23), text(1, &zero, 23));
    assert_eq!(
        fingerprint_fields_hash(1, &absent, 23),
        fingerprint_fields_hash(1, &zero, 23),
    );
}

#[test]
fn typed_fingerprint_is_a_set_and_keeps_actual_filing() {
    let first = entry();
    let mut second = first.clone();
    second.address = ElementEntry::from_parts(18, 256, 8192, Some(9), None);
    let expected = bucket_generation_digests_from_entries(1, &[first.clone(), second.clone()]);
    assert_eq!(expected.len(), 1);
    assert!(expected.contains_key(&23));
    assert_eq!(
        expected,
        bucket_generation_digests_from_entries(1, &[second, first.clone(), first.clone()]),
    );
    let mut moved = first.clone();
    moved.filed_routing_bucket = 24;
    let moved_digest = bucket_generation_digests_from_entries(1, &[moved.clone()]);
    assert!(moved_digest.contains_key(&24));
    assert!(!moved_digest.contains_key(&23));
    assert_ne!(
        moved_digest[&24],
        bucket_generation_digests_from_entries(1, &[first.clone()])[&23],
    );
    moved.filing_is_known = false;
    let legacy_bucket = bucket_for_object(&moved.object_key, 0, u32::MAX);
    assert!(bucket_generation_digests_from_entries(1, &[moved]).contains_key(&legacy_bucket));
    assert!(bucket_generation_digests_from_entries(1, &[]).is_empty());
}

fn textual_digests(entries: &[LiveBlockEntry]) -> BTreeMap<u32, BucketFingerprintDigest> {
    let mut buckets = BTreeMap::<u32, Vec<(u64, u64)>>::new();
    let mut line = String::new();
    for entry in entries {
        let bucket = entry
            .filed_bucket()
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        line.clear();
        write_bucket_fingerprint_line(&mut line, 1, entry, bucket);
        buckets
            .entry(bucket)
            .or_default()
            .push(fingerprint_line_hash(&line));
    }
    buckets
        .into_iter()
        .map(|(bucket, mut hashes)| {
            hashes.sort_unstable();
            hashes.dedup();
            (bucket, BucketFingerprintDigest::of_sorted(&hashes))
        })
        .collect()
}

#[test]
#[ignore = "bounded CPU benchmark; run in release mode"]
fn typed_fingerprint_cpu_measurement() {
    use std::hint::black_box;
    use std::time::{Duration, Instant};
    assert!(
        !cfg!(debug_assertions),
        "run this measurement in release mode"
    );
    for count in [4_000, 20_000, 100_000] {
        let entries = (0..count)
            .map(|index| {
                let mut entry = entry();
                entry.kind = StoredModelKind::ALL[index % StoredModelKind::ALL.len()];
                entry.object_key = Arc::from(format!("object-{index:08}"));
                entry.element_name = Some(Arc::from(format!("element-{}", index % 31)));
                entry.filed_routing_bucket = (index % 1024) as u32;
                entry
            })
            .collect::<Vec<_>>();
        let mut old = Vec::new();
        let mut new = Vec::new();
        for round in 0..8 {
            for typed in if round % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let start = Instant::now();
                for _ in 0..10 {
                    if typed {
                        black_box(bucket_generation_digests_from_entries(
                            1,
                            black_box(&entries),
                        ));
                    } else {
                        black_box(textual_digests(black_box(&entries)));
                    }
                }
                let elapsed = start.elapsed();
                if typed {
                    new.push(elapsed)
                } else {
                    old.push(elapsed)
                }
            }
        }
        old.sort();
        new.sort();
        let median = |times: &[Duration]| times[times.len() / 2].as_micros();
        println!(
            "blocks={count} rounds=10 samples=8 textual_us={} typed_us={}",
            median(&old),
            median(&new)
        );
    }
}
