// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn mutable_signature_lists_keep_exact_order_through_repeated_updates() {
    let mut table = SignatureTable::new(512);
    let mut active = HashMap::new();
    let mut expected = BTreeMap::<u16, Vec<u32>>::new();
    for step in 0..2048u32 {
        let position = step.wrapping_mul(7919) % 256;
        if let Some(bucket) = active.remove(&position) {
            table.remove(bucket, position);
            expected
                .get_mut(&bucket)
                .unwrap()
                .retain(|row| *row != position);
        }
        if step % 3 != 0 {
            let bucket = [0, 17, 4095][(step / 3) as usize % 3];
            table.insert(bucket, position);
            active.insert(position, bucket);
            expected.entry(bucket).or_default().insert(0, position);
        }
        table.remove(17, 511);
        for bucket in [0, 17, 4095] {
            let mut actual = Vec::new();
            assert!(!table.visit(bucket, |row| {
                actual.push(row);
                true
            }));
            assert_eq!(actual, expected.get(&bucket).cloned().unwrap_or_default());
        }
    }
}

fn populated(count: usize) -> EmbeddingIndex {
    let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(384, count)).unwrap();
    let values = vec![1.0; 384];
    for start in (0..count).step_by(256) {
        let batch = (start..(start + 256).min(count))
            .map(|item_id| EmbeddingVector {
                key: EmbeddingKey {
                    tenant_id: 7,
                    owner_id: 0,
                    item_id: item_id as u64,
                },
                values: &values,
            })
            .collect::<Vec<_>>();
        index.upsert_batch(&batch).unwrap();
    }
    index
}

#[test]
fn routed_updates_and_swap_removal_keep_every_live_row_once() {
    let mut index = populated(SIGNATURE_ROUTING_MIN_ROWS + 64);
    let values = vec![-1.0; 384];
    let key = EmbeddingKey {
        tenant_id: 7,
        owner_id: 0,
        item_id: 0,
    };
    index
        .upsert_batch(&[EmbeddingVector {
            key,
            values: &values,
        }])
        .unwrap();
    assert!(index.remove(EmbeddingKey { item_id: 1, ..key }));
    assert!(!index.remove(EmbeddingKey { item_id: 1, ..key }));
    let rows = &index.tenants[&7];
    let routing = rows.signature_routing.as_ref().unwrap();
    let signatures = rows
        .vectors
        .chunks_exact(384)
        .map(embedding_signature_i8)
        .collect::<Vec<_>>();
    for (table_index, table) in routing.tables.iter().enumerate() {
        assert_eq!(table.next.len(), rows.item_ids.len());
        assert_eq!(table.previous.len(), rows.item_ids.len());
        let mut seen = HashSet::new();
        for bucket in 0..SIGNATURE_ROUTING_BUCKETS as u16 {
            let mut previous = NO_SIGNATURE_POSITION;
            assert!(!table.visit(bucket, |position| {
                assert!(seen.insert(position), "duplicate or cyclic routing link");
                assert_eq!(table.previous[position as usize], previous);
                previous = position;
                assert_eq!(
                    signature_bucket(signatures[position as usize], table_index),
                    bucket
                );
                true
            }));
        }
        assert_eq!(seen.len(), index.len());
    }
    let hits = index.search(7, &values, 1).unwrap();
    assert_eq!(hits[0].key, key);
    assert!(hits[0].score > 0.99);
}

#[test]
fn cold_tenants_do_not_hold_inline_routing_tables() {
    let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(2, 256)).unwrap();
    for tenant_id in 0..256 {
        index
            .upsert_batch(&[EmbeddingVector {
                key: EmbeddingKey {
                    tenant_id,
                    owner_id: 0,
                    item_id: 1,
                },
                values: &[1.0, 0.0],
            }])
            .unwrap();
    }
    for rows in index.tenants.values() {
        assert!(rows.signature_routing.is_none());
        assert_eq!(
            std::mem::size_of_val(&rows.signature_routing),
            std::mem::size_of::<usize>()
        );
    }
}

#[test]
#[cfg(feature = "alloc-probe")]
#[cfg(target_os = "linux")]
#[ignore = "manual collision-heavy routing CPU and memory experiment"]
fn collision_heavy_embedding_updates() {
    const OPERATIONS: usize = 128;
    fn cpu_ns() -> u64 {
        std::fs::read_to_string("/proc/thread-self/schedstat")
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }
    eprintln!(
        "tenant_row_bytes={} routing_table_bytes={}",
        std::mem::size_of::<TenantRows>(),
        std::mem::size_of::<SignatureTable>()
    );
    for count in [8192usize, 65536, 262144] {
        let mut index = populated(count);
        let values = vec![1.0; 384];
        let mut timings = Vec::with_capacity(OPERATIONS);
        let cpu_before = cpu_ns();
        let probe = crate::alloc_probe::Probe::start();
        for item_id in 0..OPERATIONS {
            let before = std::time::Instant::now();
            let report = index
                .upsert_batch(&[EmbeddingVector {
                    key: EmbeddingKey {
                        tenant_id: 7,
                        owner_id: 0,
                        item_id: item_id as u64,
                    },
                    values: &values,
                }])
                .unwrap();
            assert_eq!(report.updated, 1);
            timings.push(before.elapsed().as_nanos() as u64);
        }
        let allocations = probe.stop();
        let cpu_elapsed = cpu_ns() - cpu_before;
        timings.sort_unstable();
        let routing = index.tenants[&7].signature_routing.as_ref().unwrap();
        let link_bytes = routing
            .tables
            .iter()
            .map(|table| {
                table.heads.len() * std::mem::size_of::<u32>()
                    + table.next.capacity() * std::mem::size_of::<u32>()
                    + table.previous.capacity() * std::mem::size_of::<u32>()
            })
            .sum::<usize>();
        for table in &routing.tables {
            let bucket = table
                .heads
                .iter()
                .position(|head| *head != NO_SIGNATURE_POSITION)
                .unwrap() as u16;
            let mut prefix = Vec::new();
            table.visit(bucket, |position| {
                prefix.push(position);
                prefix.len() < 8
            });
            assert_eq!(
                prefix,
                (OPERATIONS as u32 - 8..OPERATIONS as u32)
                    .rev()
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(allocations.allocs, 0);
        assert_eq!(index.len(), count);
        eprintln!("rows={count} ops={OPERATIONS} p50_ns={} p99_ns={} cpu_ns={cpu_elapsed} link_bytes={link_bytes} allocs={}", timings[OPERATIONS / 2], timings[OPERATIONS * 99 / 100], allocations.allocs);
    }
}
