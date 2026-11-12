// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::AddressedElementMap;
use crate::block_store::ElementEntry;
use crate::engine::{live_block_key, CarriedValue};
use std::cell::Cell;
use std::collections::BTreeMap;

thread_local! {
    static EQUALITY_CALLS: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone, Debug, Eq, Ord, PartialOrd, serde::Deserialize)]
struct CountedKey(Vec<u8>);

impl PartialEq for CountedKey {
    fn eq(&self, other: &Self) -> bool {
        EQUALITY_CALLS.with(|calls| calls.set(calls.get() + 1));
        self.0 == other.0
    }
}

fn address(slab: u64, offset: u64, length: u64) -> ElementEntry {
    ElementEntry::from_parts(slab, offset, length, Some(0), None)
}

fn key(i: usize) -> Vec<u8> {
    let mut bytes = vec![b'x'; 512];
    bytes[504..].copy_from_slice(&(i as u64).to_be_bytes());
    bytes
}

fn key_first<E: Ord, V: CarriedValue>(
    map: &BTreeMap<E, V>,
    skip: &E,
    target: &ElementEntry,
) -> bool {
    let target = live_block_key(target);
    map.iter()
        .any(|(member, value)| member != skip && live_block_key(value.carried_address()) == target)
}

#[test]
fn unrelated_blocks_do_not_compare_member_bytes() {
    for count in [0, 1, 64, 4096] {
        let map: BTreeMap<_, _> = (0..count)
            .map(|i| (CountedKey(key(i)), address(3, i as u64 * 64, 64)))
            .collect();
        let skip = CountedKey(key(count / 2));
        for target in [address(3, count as u64 / 2 * 64, 64), address(99, 0, 64)] {
            EQUALITY_CALLS.with(|calls| calls.set(0));
            let expected = key_first(&map, &skip, &target);
            let old_calls = EQUALITY_CALLS.with(Cell::get);
            EQUALITY_CALLS.with(|calls| calls.set(0));
            let actual = map.any_element_other_than_at(&skip, &target);
            let new_calls = EQUALITY_CALLS.with(Cell::get);
            assert_eq!(actual, expected);
            assert!(!actual);
            assert_eq!(old_calls, count);
            assert_eq!(
                new_calls,
                usize::from(count > 0 && target.block_slab_id() == 3)
            );
            println!("members={count} key_first_equalities={old_calls} address_first_equalities={new_calls}");
        }
    }
}

#[test]
fn ownership_uses_the_full_block_tuple_and_skip_value() {
    let at = address(3, 128, 64);
    let mut map = BTreeMap::from([
        (key(0), at.clone()),
        (key(1), address(4, 128, 64)),
        (key(2), address(3, 256, 64)),
        (key(3), address(3, 128, 128)),
    ]);
    let skip = key(0);
    assert!(!std::ptr::eq(
        map.keys().next().unwrap().as_ptr(),
        skip.as_ptr()
    ));
    assert!(!map.any_element_other_than_at(&skip, &at));
    map.insert(
        key(4),
        ElementEntry::from_parts(3, 128, 64, Some(9), Some(42)),
    );
    assert!(map.any_element_other_than_at(&skip, &at));
    let scored: BTreeMap<_, _> = map
        .iter()
        .map(|(k, v)| (k.clone(), (17u64, v.clone())))
        .collect();
    assert!(scored.any_element_other_than_at(&skip, &at));
    let positions = BTreeMap::from([(0i64, at.clone()), (1, at.clone())]);
    assert!(positions.any_element_other_than_at(&0, &at));
    for shared in [false, true] {
        let hash: crate::engine::hash_field_map::HashFieldMap = [
            ("skip".to_string(), at.clone()),
            (
                "other".to_string(),
                if shared {
                    at.clone()
                } else {
                    address(4, 128, 64)
                },
            ),
        ]
        .into_iter()
        .collect();
        let skip = std::sync::Arc::<str>::from("skip");
        assert_eq!(hash.any_element_other_than_at(&skip, &at), shared);
    }
}

#[test]
fn address_first_queries_match_key_first_through_member_churn() {
    let mut map = BTreeMap::new();
    let mut seed = 0x71e2_09abu64;
    for round in 0..1024 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let member = (seed >> 32) as usize % 128;
        let at = address((seed >> 16) % 4 + 1, (seed % 8) * 64, 64 << (round % 2));
        if round % 3 == 0 {
            map.remove(&key(member));
        } else {
            map.insert(key(member), at.clone());
        }
        for skip in [key(member), key((member + 1) % 128), key(999)] {
            assert_eq!(
                map.any_element_other_than_at(&skip, &at),
                key_first(&map, &skip, &at)
            );
        }
    }
}

#[inline(never)]
fn key_first_owner_query_workload(map: &BTreeMap<Vec<u8>, ElementEntry>, rounds: usize) -> usize {
    let rows: Vec<_> = map.iter().collect();
    (0..rounds)
        .filter(|round| {
            let (skip, at) = rows[(round * 17) % rows.len()];
            key_first(std::hint::black_box(map), skip, at)
        })
        .count()
}

#[inline(never)]
fn address_first_owner_query_workload(
    map: &BTreeMap<Vec<u8>, ElementEntry>,
    rounds: usize,
) -> usize {
    let rows: Vec<_> = map.iter().collect();
    (0..rounds)
        .filter(|round| {
            let (skip, at) = rows[(round * 17) % rows.len()];
            std::hint::black_box(map).any_element_other_than_at(skip, at)
        })
        .count()
}

#[test]
#[ignore = "standalone CPU instruction measurement"]
fn block_owner_query_cpu_fixture() {
    let map: BTreeMap<_, _> = (0..4096)
        .map(|i| (key(i), address(3, i as u64 * 64, 64)))
        .collect();
    let variant = std::env::var("MATRIXARK_OWNER_QUERY_VARIANT").unwrap();
    let started = std::time::Instant::now();
    let found = match variant.as_str() {
        "key-first" => key_first_owner_query_workload(&map, 512),
        "address-first" => address_first_owner_query_workload(&map, 512),
        _ => panic!("unknown variant"),
    };
    assert_eq!(found, 0);
    println!(
        "variant={variant} members=4096 rounds=512 key_bytes=512 elapsed_us={}",
        started.elapsed().as_micros()
    );
}
