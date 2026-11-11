// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn previous_order(
    summaries: &[BucketStorageSummary],
    claims: &BTreeMap<u32, (u64, u64)>,
) -> (Vec<u32>, usize) {
    let lookups = std::cell::Cell::new(0);
    let mut rows = summaries
        .iter()
        .filter(|row| {
            lookups.set(lookups.get() + 1);
            claims.contains_key(&row.routing_bucket)
        })
        .collect::<Vec<_>>();
    let rank = |bucket| {
        lookups.set(lookups.get() + 1);
        match claims.get(&bucket).map(|claim| claim.0) {
            Some(0) | None => u64::MAX,
            Some(sequence) => sequence,
        }
    };
    rows.sort_by(|left, right| {
        rank(left.routing_bucket)
            .cmp(&rank(right.routing_bucket))
            .then_with(|| left.last_dump_sequence.cmp(&right.last_dump_sequence))
            .then_with(|| left.routing_bucket.cmp(&right.routing_bucket))
    });
    (rows.iter().map(|row| row.routing_bucket).collect(), lookups.get())
}

fn fixture(count: u32, seed: u64) -> (Vec<BucketStorageSummary>, BTreeMap<u32, (u64, u64)>) {
    let mut summaries = Vec::new();
    let mut claims = BTreeMap::new();
    let mut random = seed;
    for routing_bucket in 0..count {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        summaries.push(BucketStorageSummary {
            routing_bucket,
            last_dump_sequence: random % 19,
            ..Default::default()
        });
        let wal = match routing_bucket % 11 {
            0 => continue,
            1 => 0,
            2 => u64::MAX,
            3 => u64::MAX - 1,
            _ => random % 97 + 1,
        };
        claims.insert(routing_bucket, (wal, random));
    }
    for index in (1..summaries.len()).rev() {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        summaries.swap(index, random as usize % (index + 1));
    }
    (summaries, claims)
}

#[test]
fn ranked_dump_order_preserves_all_ties_and_unknown_claims() {
    for count in [0, 1, 2, 31, 257, 4096] {
        for seed in [1, 7, 0x9e3779b97f4a7c15] {
            let (summaries, claims) = fixture(count, seed);
            let expected = previous_order(&summaries, &claims).0;
            let actual = oldest_dirty_buckets(&summaries, &claims);
            assert_eq!(actual, expected, "count={count} seed={seed}");
            for cap in [0, 1, 2, 64, actual.len(), actual.len() + 1] {
                assert_eq!(
                    actual.iter().take(cap).collect::<Vec<_>>(),
                    expected.iter().take(cap).collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn dump_rank_lookups_are_linear_in_candidate_count() {
    for count in [4096, 32768] {
        let (summaries, claims) = fixture(count, 0x123456789abcdef);
        let (expected, previous_lookups) = previous_order(&summaries, &claims);
        DUMP_RANK_LOOKUPS.with(|lookups| lookups.set(0));
        let actual = oldest_dirty_buckets(&summaries, &claims);
        let lookups = DUMP_RANK_LOOKUPS.with(std::cell::Cell::get);
        assert_eq!(actual, expected);
        assert_eq!(lookups, count as usize);
        assert!(previous_lookups > lookups * 4, "the control did not expose repeated lookups");
        assert!(actual.capacity() < count as usize * 2, "rank tuples leaked into output capacity");
        eprintln!("DUMP_ORDER candidates={count} selected={} lookups={lookups} previous_lookups={previous_lookups}", actual.len());
    }
}
