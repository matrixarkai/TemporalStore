// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[cfg(test)]
pub(super) fn find<'a, T: 'a>(
    affected: &[T],
    survivors: impl IntoIterator<Item = &'a T> + Copy,
    bucket: impl Fn(&T) -> u32,
) -> Vec<u32> {
    find_by(affected, &bucket, |wanted| {
        survivors.into_iter().any(|row| bucket(row) == wanted)
    })
}

pub(super) fn find_by<T>(
    affected: &[T],
    bucket: impl Fn(&T) -> u32,
    has_survivor: impl Fn(u32) -> bool,
) -> Vec<u32> {
    // Only affected buckets are queried by the caller; keep scratch bounded by that set.
    let mut buckets: Vec<_> = affected.iter().map(&bucket).collect();
    buckets.sort_unstable();
    buckets.dedup();
    buckets.retain(|wanted| has_survivor(*wanted));
    buckets
}

#[cfg(test)]
mod tests {
    use super::find;
    use std::cell::Cell;

    #[test]
    fn only_affected_buckets_with_survivors_are_returned() {
        assert_eq!(
            find(&[9, 2, 9, 4], &[8, 9, 7, 2, 2], |row| *row),
            vec![2, 9]
        );
        assert!(find(&[1, 2], &[], |row| *row).is_empty());
        assert!(find(&[], &[1, 2], |row| *row).is_empty());
        assert!(find(&[3], &[1, 2], |row| *row).is_empty());
    }

    #[test]
    fn agrees_with_whole_object_membership_for_all_small_sequences() {
        fn rows(mut value: usize) -> Vec<u32> {
            let mut rows = Vec::new();
            while value > 0 {
                rows.push((value % 4) as u32);
                value /= 4;
            }
            rows
        }
        for left in 0..256 {
            let affected = rows(left);
            for right in 0..256 {
                let survivors = rows(right);
                let actual = find(&affected, &survivors, |row| *row);
                for bucket in 0..5 {
                    assert_eq!(
                        actual.contains(&bucket),
                        affected.contains(&bucket) && survivors.contains(&bucket),
                        "affected={affected:?} survivors={survivors:?} bucket={bucket}"
                    );
                }
                assert!(actual.windows(2).all(|pair| pair[0] < pair[1]));
            }
        }
    }

    #[test]
    fn repeated_wide_survivors_stop_after_the_first_match() {
        for count in [1, 64, 4096, 16384] {
            let survivors = vec![7; count];
            let visits = Cell::new(0);
            let result = find(&[7, 7], &survivors, |row| {
                visits.set(visits.get() + 1);
                *row
            });
            assert_eq!(result, vec![7]);
            assert_eq!(visits.get(), 3, "two affected rows and one survivor");
            assert!(result.capacity() <= 2);
            println!(
                "survivors={count} inspected={} scratch_capacity={}",
                visits.get(),
                result.capacity()
            );
        }
    }

    #[test]
    fn routing_bucket_not_handle_determines_survival() {
        let affected = [(u32::MAX, 11_u64), (0, 22), (73, 33)];
        let survivors = [(u32::MAX, 44_u64), (0, 55), (91, 33)];
        assert_eq!(find(&affected, &survivors, |row| row.0), vec![0, u32::MAX]);
    }

    #[test]
    fn borrowed_tree_survivors_keep_bounded_scratch_and_short_circuit() {
        let survivors: std::collections::BTreeSet<_> = (7..16_391u32).collect();
        let visits = Cell::new(0);
        let result = find(&[7, 7], &survivors, |row| {
            visits.set(visits.get() + 1);
            *row
        });
        assert_eq!(result, vec![7]);
        assert_eq!(visits.get(), 3);
        assert!(result.capacity() <= 2);
        assert_eq!(find(&[6, 8, 17_000], &survivors, |row| *row), vec![8]);
    }
}
