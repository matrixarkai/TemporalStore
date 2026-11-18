// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn row(item_id: u64, values: &[f32]) -> EmbeddingVector<'_> {
    EmbeddingVector {
        key: EmbeddingKey {
            tenant_id: 7,
            owner_id: 9,
            item_id,
        },
        values,
    }
}

fn compare(fast: &EmbeddingIndexRegistry, prepared: &EmbeddingIndexRegistry) {
    assert_eq!(fast.len(), prepared.len());
    assert_eq!(fast.vector_bytes(), prepared.vector_bytes());
    assert_eq!(fast.space_order, prepared.space_order);
    for channel in [EmbeddingChannel::Node, EmbeddingChannel::Event] {
        for model in [0, 11, 22] {
            for query in [&[1.0, 0.0][..], &[0.0, 1.0, 0.0][..]] {
                assert_eq!(
                    fast.search(channel, 7, model, query, 32).unwrap(),
                    prepared.search(channel, 7, model, query, 32).unwrap(),
                );
            }
        }
    }
}

#[test]
fn direct_single_rows_match_prepared_updates_models_and_eviction() {
    for max_entries in [1, 3, 32] {
        for max_spaces in [1, 4] {
            let mut fast = EmbeddingIndexRegistry::with_limits(max_entries, max_spaces);
            let mut prepared = EmbeddingIndexRegistry::with_limits(max_entries, max_spaces);
            for (channel, model, item, values, only_missing) in [
                (EmbeddingChannel::Node, 11, 1, &[1.0, 0.0][..], false),
                (EmbeddingChannel::Node, 11, 2, &[0.0, 1.0][..], false),
                (EmbeddingChannel::Node, 11, 1, &[0.2, 0.8][..], false),
                (EmbeddingChannel::Node, 11, 1, &[0.9, 0.1][..], true),
                (EmbeddingChannel::Event, 11, 1, &[1.0, 0.0][..], false),
                (EmbeddingChannel::Node, 0, 3, &[1.0, 0.0][..], false),
                (EmbeddingChannel::Node, 22, 1, &[0.0, 1.0][..], false),
                (EmbeddingChannel::Node, 22, 2, &[1.0, 0.0][..], true),
                (EmbeddingChannel::Node, 22, 1, &[1.0, 0.0, 0.0][..], false),
                (EmbeddingChannel::Node, 11, 4, &[1.0, 0.0][..], false),
            ] {
                let batch = [row(item, values)];
                let actual = fast.upsert(channel, model, &batch, only_missing).unwrap();
                let expected = prepared
                    .upsert_prepared(channel, model, &batch, only_missing)
                    .unwrap();
                assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
                compare(&fast, &prepared);
            }
        }
    }
}

#[test]
fn invalid_single_row_does_not_remove_other_model_or_channel_evidence() {
    let mut registry = EmbeddingIndexRegistry::with_limits(16, 8);
    for (channel, model, item) in [
        (EmbeddingChannel::Node, 11, 1),
        (EmbeddingChannel::Node, 22, 2),
        (EmbeddingChannel::Event, 11, 1),
    ] {
        registry
            .upsert_batch(channel, model, &[row(item, &[1.0, 0.0])])
            .unwrap();
    }
    for invalid in [
        &[f32::NAN, 1.0][..],
        &[f32::INFINITY, 1.0][..],
        &[0.0, 0.0][..],
    ] {
        assert!(registry
            .upsert_batch(EmbeddingChannel::Node, 22, &[row(1, invalid)])
            .is_err());
        assert_eq!(registry.len(), 3);
        let hits = registry
            .search(EmbeddingChannel::Node, 7, 11, &[1.0, 0.0], 4)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, row(1, &[1.0, 0.0]).key);
    }
    // Existing cache-miss fills keep the stored row, even if the redundant input is invalid.
    assert!(registry
        .upsert_missing_batch(EmbeddingChannel::Node, 11, &[row(1, &[f32::NAN, 1.0])])
        .unwrap()
        .is_none());
    assert_eq!(registry.len(), 3);
}

#[test]
#[cfg(feature = "alloc-probe")]
#[cfg(target_os = "linux")]
#[ignore = "manual single-threaded embedding publication allocation and CPU experiment"]
fn single_row_publication_scale() {
    const DIMENSIONS: usize = 384;
    const OPERATIONS: usize = 4096;
    fn cpu_ns() -> u64 {
        std::fs::read_to_string("/proc/thread-self/schedstat")
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }
    fn vector(seed: usize) -> Vec<f32> {
        let mut state = seed as u64 + 1;
        (0..DIMENSIONS)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                ((state >> 32) as i32) as f32 / i32::MAX as f32
            })
            .collect()
    }
    for count in [8192usize, 65536, 262144] {
        let mut fast = EmbeddingIndexRegistry::new(count + 1);
        let mut prepared = EmbeddingIndexRegistry::new(count + 1);
        for start in (0..count).step_by(256) {
            let vectors = (start..start + 256).map(vector).collect::<Vec<_>>();
            let batch = vectors
                .iter()
                .enumerate()
                .map(|(offset, values)| row((start + offset) as u64, values))
                .collect::<Vec<_>>();
            fast.upsert_batch(EmbeddingChannel::Node, 11, &batch)
                .unwrap();
            prepared
                .upsert_prepared(EmbeddingChannel::Node, 11, &batch, false)
                .unwrap();
        }
        for (label, registry) in [("prepared", &mut prepared), ("direct", &mut fast)] {
            let vectors = (0..OPERATIONS)
                .map(|index| vector(index + count))
                .collect::<Vec<_>>();
            let mut timings = Vec::with_capacity(OPERATIONS);
            let cpu_before = cpu_ns();
            let probe = crate::alloc_probe::Probe::start();
            for (index, values) in vectors.iter().enumerate() {
                let key = (index * 7919 % count) as u64;
                let batch = [row(key, values)];
                let before = std::time::Instant::now();
                let report = if label == "direct" {
                    registry.upsert_batch(EmbeddingChannel::Node, 11, &batch)
                } else {
                    registry.upsert_prepared(EmbeddingChannel::Node, 11, &batch, false)
                }
                .unwrap()
                .unwrap();
                assert_eq!(report.updated, 1);
                timings.push(before.elapsed().as_nanos() as u64);
            }
            let allocations = probe.stop();
            let cpu_elapsed = cpu_ns() - cpu_before;
            timings.sort_unstable();
            eprintln!("rows={count} path={label} ops={OPERATIONS} p50_ns={} p99_ns={} cpu_ns={cpu_elapsed} allocs={} alloc_bytes={}", timings[OPERATIONS / 2], timings[OPERATIONS * 99 / 100], allocations.allocs, allocations.alloc_bytes);
            assert_eq!(registry.len(), count);
            if label == "direct" {
                assert_eq!(
                    allocations.allocs, 0,
                    "warm in-place publication should need no heap scratch"
                );
            } else {
                assert!(allocations.allocs >= OPERATIONS as u64);
            }
        }
        compare(&fast, &prepared);
        for seed in [0, count / 2, count - 1, count + OPERATIONS / 2] {
            let query = vector(seed);
            assert_eq!(
                fast.search(EmbeddingChannel::Node, 7, 11, &query, 32)
                    .unwrap(),
                prepared
                    .search(EmbeddingChannel::Node, 7, 11, &query, 32)
                    .unwrap(),
                "same corpus and updates must retain identical retrieved evidence",
            );
        }
    }
}
