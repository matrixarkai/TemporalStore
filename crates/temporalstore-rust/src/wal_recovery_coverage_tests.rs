// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::{every_result_can_be_found_again, StagedBlock, WalOutcomeItem};
use std::sync::Arc;

fn scalar(outcomes: &[WalOutcomeItem], blocks: &[StagedBlock]) -> bool {
    outcomes.iter().all(|item| {
        item.deleted
            || item.ttl.is_some()
            || item.value.as_ref().is_some_and(|value| !value.is_empty())
            || blocks.iter().any(|block| {
                block.object_id == item.object_id
                    && (block.element_name.as_deref() == item.element_name.as_deref()
                        || block.element_name.is_none())
            })
    })
}

fn item(object_id: u64, name: Option<&str>) -> WalOutcomeItem {
    WalOutcomeItem {
        kind: "page".into(),
        object_key: format!("object/{object_id}"),
        element_name: name.map(str::to_owned),
        object_id,
        routing_bucket: 7,
        address: None,
        value: None,
        ttl: None,
        deleted: false,
        meta: false,
    }
}

fn block(object_id: u64, name: Option<&str>) -> StagedBlock {
    StagedBlock {
        object_id,
        element_name: name.map(Arc::from),
        bytes: vec![1; 16],
    }
}

fn fixture(count: usize) -> (Vec<WalOutcomeItem>, Vec<StagedBlock>) {
    let outcomes = (0..count).map(|i| item(i as u64, Some("field"))).collect();
    let blocks = (0..count).map(|i| block(i as u64, Some("field"))).collect();
    (outcomes, blocks)
}

#[test]
fn indexed_and_scalar_coverage_agree_on_mixed_and_reordered_batches() {
    for count in [0, 1, 8, 9, 16, 63, 64, 65, 128, 500, 511, 512, 513, 1024] {
        for seed in 0..12 {
            let mut state = seed + 1_u64;
            let mut random = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            let mut outcomes = Vec::new();
            let mut blocks = Vec::new();
            for i in 0..count {
                let id = if i % 9 == 0 { u64::MAX } else { random() % 31 };
                let name = format!("field-{}", random() % 19);
                let mut result = item(id, Some(&name));
                blocks.push(block(
                    id,
                    if random() % 4 == 0 { None } else { Some(&name) },
                ));
                if i % 7 == 0 {
                    blocks.push(block(id, Some(&name)));
                }
                match random() % 8 {
                    0 => result.deleted = true,
                    1 => result.ttl = Some(0),
                    2 => result.value = Some(vec![3]),
                    3 => result.value = Some(Vec::new()),
                    _ => {}
                }
                outcomes.push(result);
            }
            blocks.reverse();
            assert!(scalar(&outcomes, &blocks));
            assert!(every_result_can_be_found_again(&outcomes, &blocks));
            if !outcomes.is_empty() {
                let last = outcomes.last_mut().unwrap();
                *last = item(u64::MAX - 1, Some("uncarried"));
                assert!(!scalar(&outcomes, &blocks));
                assert!(!every_result_can_be_found_again(&outcomes, &blocks));
            }
        }
    }
}

#[test]
fn indexed_coverage_accepts_whole_objects_and_exact_empty_names() {
    let (mut outcomes, mut blocks) = fixture(512);
    blocks.push(block(900, None));
    blocks.push(block(901, Some("")));
    outcomes.extend([
        item(900, None),
        item(900, Some("anything")),
        item(901, Some("")),
    ]);
    assert!(every_result_can_be_found_again(&outcomes, &blocks));
    outcomes.push(item(901, None));
    assert!(!every_result_can_be_found_again(&outcomes, &blocks));
    outcomes.pop();
    outcomes.push(item(901, Some("another")));
    assert!(!every_result_can_be_found_again(&outcomes, &blocks));
}

#[test]
fn indexed_coverage_requires_the_same_object_and_real_value_bytes() {
    let (mut outcomes, blocks) = fixture(512);
    let mut tail = item(900, Some("field"));
    tail.value = Some(Vec::new());
    outcomes.push(tail);
    assert!(!every_result_can_be_found_again(&outcomes, &blocks));
    outcomes.last_mut().unwrap().value = Some(vec![0]);
    assert!(every_result_can_be_found_again(&outcomes, &blocks));
    outcomes.last_mut().unwrap().value = None;
    outcomes.last_mut().unwrap().ttl = Some(0);
    assert!(every_result_can_be_found_again(&outcomes, &blocks));
    outcomes.last_mut().unwrap().ttl = None;
    outcomes.last_mut().unwrap().deleted = true;
    assert!(every_result_can_be_found_again(&outcomes, &blocks));
}

#[test]
#[cfg(feature = "alloc-probe")]
fn coverage_allocates_only_after_repeated_large_block_searches() {
    let (mut outcomes, blocks) = fixture(512);
    let probe = crate::alloc_probe::Probe::start();
    let covered = every_result_can_be_found_again(&outcomes[..8], &blocks);
    let counts = probe.stop();
    assert!(covered);
    assert_eq!(counts.allocs, 0);

    for outcome in &mut outcomes {
        outcome.value = Some(vec![1]);
    }
    let probe = crate::alloc_probe::Probe::start();
    let covered = every_result_can_be_found_again(&outcomes, &blocks);
    let counts = probe.stop();
    assert!(covered);
    assert_eq!(counts.allocs, 0);

    for outcome in &mut outcomes {
        outcome.value = None;
    }
    outcomes[0].object_id = 900;
    let probe = crate::alloc_probe::Probe::start();
    let covered = every_result_can_be_found_again(&outcomes, &blocks);
    let counts = probe.stop();
    assert!(!covered);
    assert_eq!(counts.allocs, 0);

    outcomes[0].object_id = 0;
    let probe = crate::alloc_probe::Probe::start();
    let covered = every_result_can_be_found_again(&outcomes, &blocks);
    let counts = probe.stop();
    assert!(covered);
    assert!(counts.allocs > 0 && counts.alloc_bytes > 0);
    assert_eq!(counts.alloc_bytes, counts.free_bytes);
}

#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn coverage_work_and_temporary_memory_by_batch_size() {
    use std::hint::black_box;
    use std::time::Instant;

    for count in [1, 8, 16, 63, 64, 128, 500, 2560] {
        let (outcomes, blocks) = fixture(count);
        let iterations = (200_000 / count).max(20);
        let probe = crate::alloc_probe::Probe::start();
        assert!(black_box(every_result_can_be_found_again(
            black_box(&outcomes),
            black_box(&blocks)
        )));
        let allocated = probe.stop();
        for pass in 0..3 {
            let mut times = [0_u128; 2];
            for arm in if pass % 2 == 0 { [0, 1] } else { [1, 0] } {
                let start = Instant::now();
                for _ in 0..iterations {
                    let covered = if arm == 0 {
                        scalar(black_box(&outcomes), black_box(&blocks))
                    } else {
                        every_result_can_be_found_again(black_box(&outcomes), black_box(&blocks))
                    };
                    assert!(black_box(covered));
                }
                times[arm] = start.elapsed().as_nanos() / iterations as u128;
            }
            println!("COVERAGE count={count} pass={pass} scalar_ns={} indexed_ns={} allocs={} temporary_bytes={}",
                     times[0], times[1], allocated.allocs, allocated.alloc_bytes);
        }
    }
}
