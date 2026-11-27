// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A BLOCK'S CACHE KEY SPELLS ITS NUMBERS WITHOUT THE FORMATTER, AND THE SPELLING IS UNCHANGED.
//!
//! `BlockKeys::key` builds the selector of a block's cache key -- `slot-<bucket>:<offset>:<length>`
//! -- for every block a read touches. It used to run the formatting machinery for three integers per
//! key; a profile of a whole-series feature read put that at over a quarter of the instructions per
//! point (`core::fmt::write`, `pad_integral`, the `Display` impls). It now writes the digits
//! straight into the string it already sized.
//!
//! A key that differs by one byte is a silent cache miss, so the spelling is the whole contract.
//! `the_selector_is_the_formatters_spelling_at_every_digit_boundary` builds the expected selector
//! with the formatter, as the code did, and requires the key to match it byte for byte over every
//! digit-count boundary of each field, with and without a routing bucket, plus the record key and
//! namespace the key carries.
//!
//! WHAT THIS DOES NOT SHOW: the instruction saving. The allocation probes cannot see it, because the
//! selector was already sized exactly and the formatter allocates nothing. It is measured with
//! callgrind, and the numbers are in the pull request.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

#[test]
fn the_selector_is_the_formatters_spelling_at_every_digit_boundary() {
    // Each field at 0, at each power of ten and the value before it, and at its largest.
    let mut offsets: Vec<u64> = vec![0, 1, 9, 10, 99, 100, 999, 1_000, 9_999, 10_000];
    offsets.extend([99_999, 100_000, 999_999, 1_000_000, 4_294_967_295]);
    let lengths: Vec<u64> = vec![0, 1, 9, 10, 99, 100, 65_535, 65_536, 4_294_967_295];
    let buckets: Vec<Option<u32>> = vec![
        None,
        Some(0),
        Some(7),
        Some(10),
        Some(99),
        Some(100),
        Some(4_294_967_295),
    ];
    let mut checked = 0usize;
    let mut keys = crate::engine::BlockKeys::new();
    for &slab in &[0u64, 3, 4_000_000_000] {
        for &offset in &offsets {
            for &length in &lengths {
                let address = crate::ElementEntry::from_parts(slab, offset, length, Some(1), None);
                for &bucket in &buckets {
                    let key = keys.key(1, &address, bucket);
                    let want = match bucket {
                        Some(bucket) => {
                            format!("slot-{bucket}:{}:{}", address.offset(), address.length())
                        }
                        None => format!("{}:{}", address.offset(), address.length()),
                    };
                    assert_eq!(
                        want, key.selector,
                        "slab {slab} offset {offset} length {length} bucket {bucket:?}"
                    );
                    assert_eq!("page", key.namespace);
                    assert_eq!(
                        matrixcache::CacheKey::page_with_slot(1, slab, 0, 0, None).record_key,
                        key.record_key,
                        "the record key moved for slab {slab}"
                    );
                    checked += 1;
                }
            }
        }
    }
    // A loop that ran no case, or one over a shrunken table, would pass; the count says it did not.
    assert_eq!(3 * offsets.len() * lengths.len() * buckets.len(), checked);
}
