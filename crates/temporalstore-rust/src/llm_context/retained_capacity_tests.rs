// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn key(tenant_id: u64, item_id: u64) -> EmbeddingKey {
    EmbeddingKey {
        tenant_id,
        owner_id: 0,
        item_id,
    }
}

fn seed(index: &mut EmbeddingIndex, tenant: u64, count: usize, vector: &[f32]) {
    let rows = (0..count)
        .map(|item| EmbeddingVector {
            key: key(tenant, item as u64),
            values: vector,
        })
        .collect::<Vec<_>>();
    index.upsert_batch(&rows).unwrap();
}

fn assert_sparse_capacity(rows: &TenantRows) {
    assert!(
        rows.vectors.capacity() <= rows.vectors.len() * 4,
        "vector capacity {} for {} live bytes",
        rows.vectors.capacity(),
        rows.vectors.len()
    );
    assert!(rows.scales.capacity() <= rows.scales.len() * 4);
    assert!(rows.owner_ids.capacity() <= rows.owner_ids.len() * 4);
    assert!(rows.item_ids.capacity() <= rows.item_ids.len() * 4);
    if let IdentityPositions::Small(positions) = &rows.positions {
        assert!(positions.capacity() <= positions.len() * 4);
    }
}

#[test]
fn deletion_releases_sparse_capacity_without_losing_the_survivor() {
    let vector = vec![1.0; 384];
    let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(384, 2_048)).unwrap();
    seed(&mut index, 7, 2_048, &vector);
    let peak = index.tenants[&7].vectors.capacity();
    assert!(peak >= 2_048 * vector.len());
    for item in 1..2_048 {
        assert!(index.remove(key(7, item)));
    }
    assert_eq!(index.len(), 1);
    assert_sparse_capacity(&index.tenants[&7]);
    let hits = index.search(7, &vector, 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].key, key(7, 0));
    assert!((hits[0].score - 1.0).abs() < 0.00001);
    assert!(!index.remove(key(7, 999_999)));
    assert!(index.remove(key(7, 0)));
    assert!(index.tenants.is_empty());
}

#[test]
fn tenant_churn_retains_capacity_proportional_to_live_rows() {
    let vector = vec![1.0; 64];
    let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(64, 1_024)).unwrap();
    for tenant in 0..8 {
        seed(&mut index, tenant, 512, &vector);
        for item in 1..512 {
            assert!(index.remove(key(tenant, item)));
        }
        for rows in index.tenants.values() {
            assert_sparse_capacity(rows);
        }
    }
    assert_eq!(index.len(), 8);
    let allocated = index
        .tenants
        .values()
        .map(|rows| rows.vectors.capacity())
        .sum::<usize>();
    assert!(allocated <= 4 * index.len() * vector.len());
    for tenant in 0..8 {
        assert_eq!(
            index.search(tenant, &vector, 10).unwrap()[0].key,
            key(tenant, 0)
        );
    }
    seed(&mut index, 0, 128, &vector);
    assert_eq!(index.len(), 135);
    assert_eq!(index.search(0, &vector, 200).unwrap().len(), 128);
}

#[test]
fn sparse_trimming_preserves_survivor_ranking_and_allows_refill() {
    let vectors = (0..128)
        .map(|item| {
            vec![
                1.0,
                (item % 7) as f32 + 1.0,
                (item % 11) as f32 - 5.0,
                item as f32 / 128.0,
            ]
        })
        .collect::<Vec<_>>();
    let mut index = EmbeddingIndex::new(EmbeddingIndexConfig::new(4, 256)).unwrap();
    let batch = vectors
        .iter()
        .enumerate()
        .map(|(item, values)| EmbeddingVector {
            key: key(9, item as u64),
            values,
        })
        .collect::<Vec<_>>();
    index.upsert_batch(&batch).unwrap();
    let query = [0.25, 0.5, 1.0, 0.75];
    let expected = index
        .search(9, &query, 128)
        .unwrap()
        .into_iter()
        .filter(|hit| hit.key.item_id % 16 == 0)
        .collect::<Vec<_>>();
    let capacity = index.tenants[&9].vectors.capacity();
    let address = index.tenants[&9].vectors.as_ptr();
    assert!(index.remove(key(9, 1)));
    assert_eq!(index.tenants[&9].vectors.capacity(), capacity);
    assert_eq!(
        index.tenants[&9].vectors.as_ptr(),
        address,
        "ordinary individual deletion must not reallocate"
    );
    for item in 2..128 {
        if item % 16 != 0 {
            assert!(index.remove(key(9, item)));
        }
    }
    assert_sparse_capacity(&index.tenants[&9]);
    assert_eq!(index.search(9, &query, 128).unwrap(), expected);
    let new_vector = [0.1, 0.2, 0.3, 0.4];
    index
        .upsert_batch(&[EmbeddingVector {
            key: key(9, 200),
            values: &new_vector,
        }])
        .unwrap();
    let after = index
        .search(9, &query, 128)
        .unwrap()
        .into_iter()
        .filter(|hit| hit.key.item_id != 200)
        .collect::<Vec<_>>();
    assert_eq!(after, expected);
}

#[test]
fn trimming_signature_buffers_keeps_collision_links_and_order() {
    let mut table = SignatureTable::new(512);
    for position in 0..512u32 {
        table.insert((position % 3) as u16, position);
    }
    for position in 8..512u32 {
        table.remove((position % 3) as u16, position);
    }
    table.next.truncate(8);
    table.previous.truncate(8);
    let collect = |table: &SignatureTable| {
        (0..3)
            .map(|bucket| {
                let mut positions = Vec::new();
                table.visit(bucket, |position| {
                    positions.push(position);
                    true
                });
                positions
            })
            .collect::<Vec<_>>()
    };
    let expected = collect(&table);
    trim_sparse_buffer(&mut table.next);
    trim_sparse_buffer(&mut table.previous);
    assert!(table.next.capacity() <= 32);
    assert!(table.previous.capacity() <= 32);
    assert_eq!(collect(&table), expected);
    table.remove(1, 7);
    table.insert(0, 7);
    assert!(!collect(&table)[1].contains(&7));
    assert_eq!(collect(&table)[0][0], 7);
}
