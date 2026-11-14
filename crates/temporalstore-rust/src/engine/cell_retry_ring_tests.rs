// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn receipt(n: u64) -> RetryReceipt {
    RetryReceipt { snapshots_written: n, cells_created: n * 10, cells_merged: n * 100 }
}

/// A ring after tokens `1..=n` were applied in order, each with the receipt `receipt(token)`.
fn ring_of(n: u64) -> RetryRing {
    let mut ring = RetryRing::new();
    for token in 1..=n {
        ring.record(token, receipt(token)).unwrap();
    }
    ring
}

#[test]
fn a_remembered_token_replays_its_first_receipt_and_a_new_one_is_fresh() {
    let mut ring = RetryRing::new();
    assert_eq!(ring.admit(7), Admission::Fresh);
    ring.record(7, receipt(3)).unwrap();
    assert_eq!(ring.admit(7), Admission::Replay(receipt(3)));
    assert_eq!(ring.admit(8), Admission::Fresh);
    // The token zero is a token like any other.
    assert_eq!(ring.admit(0), Admission::Fresh);
    ring.record(0, receipt(4)).unwrap();
    assert_eq!(ring.admit(0), Admission::Replay(receipt(4)));
    assert_eq!(ring.admit(7), Admission::Replay(receipt(3)));
}

#[test]
fn the_capacity_is_sixteen_and_a_full_ring_holds_exactly_that_many() {
    assert_eq!(RETRY_RING_CAPACITY, 16);
    let ring = ring_of(RETRY_RING_CAPACITY as u64);
    assert_eq!(ring.len(), 16);
    assert_eq!(ring.tokens(), (1..=16).collect::<Vec<u64>>());
    // At exactly the limit nothing has been evicted: the oldest is still protected.
    assert_eq!(ring.admit(1), Admission::Replay(receipt(1)));
    assert_eq!(ring.admit(16), Admission::Replay(receipt(16)));
}

#[test]
fn the_token_past_the_limit_evicts_the_oldest_and_nothing_else() {
    let mut ring = ring_of(16);
    let evicted = ring.record(17, receipt(17)).unwrap();
    assert_eq!(evicted, Some(1));
    assert_eq!(ring.len(), 16, "the ring grew past its capacity");
    assert_eq!(ring.tokens(), (2..=17).collect::<Vec<u64>>());
    // A retry older than the capacity is NOT protected: it is fresh again, and is applied.
    assert_eq!(ring.admit(1), Admission::Fresh);
    for token in 2..=17 {
        assert_eq!(ring.admit(token), Admission::Replay(receipt(token)), "token {token}");
    }
    // Applying that unprotected retry evicts the next oldest, in order.
    assert_eq!(ring.record(1, receipt(99)).unwrap(), Some(2));
    assert_eq!(ring.admit(1), Admission::Replay(receipt(99)));
    assert_eq!(ring.admit(2), Admission::Fresh);
    // Below the limit there is no eviction.
    let mut small = ring_of(15);
    assert_eq!(small.record(16, receipt(16)).unwrap(), None);
}

#[test]
fn eviction_follows_arrival_order_over_a_long_run() {
    let mut ring = RetryRing::new();
    for token in 1..=1000u64 {
        let evicted = ring.record(token, receipt(token)).unwrap();
        assert_eq!(evicted, token.checked_sub(16).filter(|t| *t >= 1), "token {token}");
    }
    assert_eq!(ring.tokens(), (985..=1000).collect::<Vec<u64>>());
}

#[test]
fn a_token_already_held_is_refused_and_changes_nothing() {
    let mut ring = ring_of(5);
    let before = ring.clone();
    let err = ring.record(3, receipt(77)).unwrap_err();
    assert!(err.contains("already recorded"), "{err}");
    assert_eq!(ring, before);
    // Asking never moves an entry either: the retried token stays where it was.
    for _ in 0..3 {
        assert_eq!(ring.admit(1), Admission::Replay(receipt(1)));
    }
    assert_eq!(ring, before);
    assert_eq!(ring.tokens(), vec![1, 2, 3, 4, 5]);
}

#[test]
fn two_tokens_interleaved_and_one_token_twice_apart_each_apply_once() {
    // Applies a request the way the write path will: ask, apply on Fresh, record.
    fn send(ring: &mut RetryRing, applied: &mut Vec<u64>, token: u64) -> RetryReceipt {
        match ring.admit(token) {
            Admission::Replay(first) => first,
            Admission::Fresh => {
                applied.push(token);
                let landed = receipt(applied.len() as u64);
                ring.record(token, landed).unwrap();
                landed
            }
        }
    }
    let mut ring = RetryRing::new();
    let mut applied = Vec::new();
    let a1 = send(&mut ring, &mut applied, 10);
    let b1 = send(&mut ring, &mut applied, 20);
    let a2 = send(&mut ring, &mut applied, 10);
    let b2 = send(&mut ring, &mut applied, 20);
    let c = send(&mut ring, &mut applied, 30);
    let a3 = send(&mut ring, &mut applied, 10);
    assert_eq!(applied, vec![10, 20, 30], "a repeated token was applied again");
    assert_eq!((a1, a2, a3), (receipt(1), receipt(1), receipt(1)));
    assert_eq!((b1, b2), (receipt(2), receipt(2)));
    assert_eq!(c, receipt(3));
    assert_eq!(ring.tokens(), vec![10, 20, 30], "a replay reordered the ring");
}

#[test]
fn the_same_requests_give_the_same_bytes_and_a_decoded_ring_continues_as_the_original_would() {
    let mut first = RetryRing::new();
    let mut second = RetryRing::new();
    for token in (0..40u64).map(|i| i * 7919 % 101) {
        for ring in [&mut first, &mut second] {
            if ring.admit(token) == Admission::Fresh {
                ring.record(token, receipt(token + 1)).unwrap();
            }
        }
    }
    assert_eq!(first.encode(), second.encode());
    assert_eq!(first, second);
    // A replay (a restart that read the stored ring) and a leader that never stopped agree.
    let mut revived = RetryRing::decode(&first.encode()).unwrap();
    assert_eq!(revived, first);
    for token in [500u64, 501, 500, 3, 502] {
        for ring in [&mut first, &mut revived] {
            if ring.admit(token) == Admission::Fresh {
                ring.record(token, receipt(token)).unwrap();
            }
        }
        assert_eq!(first.encode(), revived.encode(), "after token {token}");
    }
}

#[test]
fn an_empty_ring_holds_nothing_and_stores_no_bytes() {
    let ring = RetryRing::new();
    assert!(ring.is_empty());
    assert_eq!(ring.len(), 0);
    assert_eq!(ring.encode(), Vec::<u8>::new());
    assert_eq!(ring.entries.capacity(), 0, "an empty ring reserved memory");
    assert_eq!(RetryRing::decode(&[]).unwrap(), ring);
    assert_eq!(ring.admit(1), Admission::Fresh);
    // The first record reserves the whole ring at once, and a full ring never grows.
    let mut ring = RetryRing::new();
    ring.record(1, receipt(1)).unwrap();
    let reserved = ring.entries.capacity();
    assert!(reserved >= 16);
    for token in 2..=100 {
        ring.record(token, receipt(token)).unwrap();
    }
    assert_eq!(ring.entries.capacity(), reserved);
}

#[test]
fn the_stored_bytes_are_the_hand_written_ones_and_round_trip() {
    let mut ring = RetryRing::new();
    ring.record(5, RetryReceipt { snapshots_written: 1, cells_created: 2, cells_merged: 3 }).unwrap();
    ring.record(300, RetryReceipt { snapshots_written: 0, cells_created: 0, cells_merged: 128 }).unwrap();
    let want: Vec<u8> = vec![
        0x0a, 0x08, 0x08, 0x05, 0x10, 0x01, 0x18, 0x02, 0x20, 0x03, // token 5: 1, 2, 3
        0x0a, 0x0a, 0x08, 0xac, 0x02, 0x10, 0x00, 0x18, 0x00, 0x20, 0x80, 0x01, // token 300: 0, 0, 128
    ];
    assert_eq!(ring.encode(), want);
    assert_eq!(RetryRing::decode(&want).unwrap(), ring);
    // The extreme values a counter can reach survive the trip.
    let mut wide = RetryRing::new();
    let big = RetryReceipt { snapshots_written: u64::MAX, cells_created: u64::MAX - 1, cells_merged: 1 << 63 };
    wide.record(u64::MAX, big).unwrap();
    assert_eq!(RetryRing::decode(&wide.encode()).unwrap().admit(u64::MAX), Admission::Replay(big));
}

fn entry(token: u64) -> Vec<u8> {
    let mut one = RetryRing::new();
    one.record(token, receipt(token)).unwrap();
    one.encode()
}

#[test]
fn a_stored_ring_of_sixteen_is_read_and_one_of_seventeen_is_refused() {
    let at_limit: Vec<u8> = (1..=16).flat_map(entry).collect();
    let ring = RetryRing::decode(&at_limit).unwrap();
    assert_eq!(ring.len(), 16);
    assert_eq!(ring.tokens(), (1..=16).collect::<Vec<u64>>());
    let past: Vec<u8> = (1..=17).flat_map(entry).collect();
    let err = RetryRing::decode(&past).unwrap_err();
    assert!(err.contains("more than 16"), "{err}");
}

#[test]
fn a_token_twice_in_one_stored_ring_is_refused() {
    let twice: Vec<u8> = [1u64, 2, 1].into_iter().flat_map(entry).collect();
    let err = RetryRing::decode(&twice).unwrap_err();
    assert!(err.contains("already recorded"), "{err}");
}

#[test]
fn a_value_that_is_not_the_canonical_spelling_is_refused() {
    let good = entry(5);
    assert!(RetryRing::decode(&good).is_ok());
    // An unknown outer field, and the right field with the wrong wire type.
    assert!(RetryRing::decode(&[0x12, 0x00]).is_err());
    assert!(RetryRing::decode(&[0x08, 0x05]).is_err());
    // Trailing bytes after an entry's last field (the entry's own length covers them).
    let mut inner = good[2..].to_vec();
    inner.extend([0x28, 0x01]);
    let mut padded = vec![0x0a, inner.len() as u8];
    padded.extend(&inner);
    assert!(RetryRing::decode(&padded).unwrap_err().contains("after its last field"));
    // A missing field, a repeated field, fields out of order.
    for body in [
        vec![0x08, 0x05, 0x10, 0x01, 0x18, 0x02],
        vec![0x08, 0x05, 0x08, 0x05, 0x10, 0x01, 0x18, 0x02, 0x20, 0x03],
        vec![0x10, 0x01, 0x08, 0x05, 0x18, 0x02, 0x20, 0x03],
    ] {
        let mut value = vec![0x0a, body.len() as u8];
        value.extend(&body);
        assert!(RetryRing::decode(&value).is_err(), "{body:?}");
    }
    // A padded varint reads to the same number but is not what `encode` writes.
    let padded_varint = vec![0x0a, 0x09, 0x08, 0x85, 0x00, 0x10, 0x01, 0x18, 0x02, 0x20, 0x03];
    assert_eq!(padded_varint[1] as usize, padded_varint.len() - 2);
    let err = RetryRing::decode(&padded_varint).unwrap_err();
    assert!(err.contains("canonical"), "{err}");
}

#[test]
fn a_truncated_or_corrupt_value_is_an_error_and_never_a_panic() {
    let full: Vec<u8> = (1..=16).flat_map(entry).collect();
    for cut in 0..full.len() {
        let result = RetryRing::decode(&full[..cut]);
        // Only a cut on an entry boundary is a valid shorter ring.
        let boundary = (0..=16).any(|n| (1..=n).flat_map(entry).count() == cut);
        assert_eq!(result.is_ok(), boundary, "cut at {cut}");
    }
    for position in 0..full.len() {
        let mut flipped = full.clone();
        flipped[position] ^= 0xff;
        let _ = RetryRing::decode(&flipped);
    }
    assert!(RetryRing::decode(&[0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]).is_err());
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn allocations<R>(work: impl FnOnce() -> R) -> (u64, R) {
        let probe = Probe::start();
        let out = work();
        (probe.stop().allocs, out)
    }

    /// Asking allocates nothing at any fill, and a record into a ring that is already full allocates
    /// nothing: the cost does not move with how many tokens have gone through the ring.
    /// THE CONTROL: a ring that rebuilt itself on every record (what a `Vec` that is cloned to evict
    /// would do) is counted by the same probe and moves with every record.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_retry_ring_allocates_once_when_started_and_never_again() {
        let mut ring = RetryRing::new();
        let (first, _) = allocations(|| ring.record(1, receipt(1)).unwrap());
        println!("first record: {first}");
        assert_eq!(first, 1, "the first record reserves the ring in one allocation");
        for token in 2..=16 {
            let (spent, _) = allocations(|| ring.record(token, receipt(token)).unwrap());
            assert_eq!(spent, 0, "record {token} into a ring with room");
        }
        let mut worst_full = 0;
        for token in 17..=4000u64 {
            let (spent, evicted) = allocations(|| ring.record(token, receipt(token)).unwrap());
            assert_eq!(evicted, Some(token - 16));
            worst_full = worst_full.max(spent);
        }
        let (asked, _) = allocations(|| {
            for token in 3900..4100u64 {
                std::hint::black_box(ring.admit(token));
            }
        });
        println!("full-ring record worst {worst_full}, 200 admits {asked}");
        assert_eq!(worst_full, 0, "a record into a full ring allocated");
        assert_eq!(asked, 0, "asking allocated");

        // CONTROL: the rebuilding shape is visible to the probe and grows with the records.
        let mut naive: Vec<(u64, RetryReceipt)> = Vec::new();
        let (churn, _) = allocations(|| {
            for token in 0..1000u64 {
                let mut next = naive.clone();
                if next.len() >= 16 {
                    next.remove(0);
                }
                next.push((token, receipt(token)));
                naive = next;
            }
        });
        println!("control: 1000 rebuilds {churn}");
        assert!(churn >= 900, "CONTROL: the probe did not see the rebuilding ring ({churn})");
    }
}
