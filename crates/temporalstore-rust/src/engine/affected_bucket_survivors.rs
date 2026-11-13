// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(super) fn find<T>(affected: &[T], survivors: &[T], bucket: impl Fn(&T) -> u32) -> Vec<u32> {
    // Only affected buckets are queried by the caller; keep scratch bounded by that set.
    let mut buckets: Vec<_> = affected.iter().map(&bucket).collect();
    buckets.sort_unstable();
    buckets.dedup();
    buckets.retain(|wanted| survivors.iter().any(|row| bucket(row) == *wanted));
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
}
