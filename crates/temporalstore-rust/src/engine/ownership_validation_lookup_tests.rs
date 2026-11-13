// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn scan_validation(
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    start: u32,
    end: u32,
) -> (StorageBlockOwnershipValidation, usize) {
    let mut result = StorageBlockOwnershipValidation::default();
    let mut visits = 0;
    for entry in entries {
        let object_id = expected_live_block_object_id(1, entry);
        let routing_bucket = block_routing_bucket(&entry.object_key, start, end);
        if entry.filed_bucket().is_none() {
            result.missing_owner_block_refs += 1;
        }
        let present = shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .is_some_and(|bucket| {
                bucket.object_index.contains(&object_id)
                    && bucket.block_index.values().any(|block| {
                        visits += 1;
                        block.address.block_slab_id() == entry.address.block_slab_id()
                            && block.address.offset() == entry.address.offset()
                            && block.address.length() == entry.address.length()
                            && block.address.block_id() == entry.address.block_id()
                            && block.model_id() == entry.kind
                    })
            });
        if !present {
            result.missing_owner_block_refs += 1;
        }
        if entry
            .filed_bucket()
            .is_some_and(|actual| actual != routing_bucket)
        {
            result.mismatches.push(StorageRecoveryBlockOwnerMismatch {
                object_key: entry.object_key.to_string(),
                block_slab_id: entry.address.block_slab_id(),
                offset: entry.address.offset(),
                expected_object_id: object_id,
                actual_object_id: None,
                expected_routing_bucket: routing_bucket,
                actual_routing_bucket: entry.filed_bucket(),
            });
        }
    }
    (result, visits)
}

fn fixture(count: usize) -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let result = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![Command::HashMultiSet {
            key: "wide-row".into(),
            entries: (0..count)
                .map(|i| (format!("field-{i}"), vec![7; 8]))
                .collect(),
        }],
    });
    assert!(result.status.ok);
    assert!(result.responses.iter().all(|response| response.status.ok));
    (dir, engine)
}

fn assert_same(shard: &ShardState, entries: &[LiveBlockEntry], start: u32, end: u32) {
    let expected = scan_validation(shard, entries, start, end).0;
    let actual = validate_bucket_ownership_index_from_entries(1, shard, entries, start, end);
    assert_eq!(
        actual.missing_owner_block_refs,
        expected.missing_owner_block_refs
    );
    assert_eq!(actual.mismatches, expected.mismatches);
}

#[test]
fn ownership_validation_indexes_a_wide_bucket_once() {
    for count in [1, 128, 512] {
        let (_dir, engine) = fixture(count);
        let shards = engine.shards.read().unwrap();
        let shard = shards.get(&1).unwrap();
        let entries = collect_live_block_entries(shard);
        assert_eq!(entries.len(), count);
        OWNERSHIP_INDEX_BLOCK_VISITS.with(|visits| visits.set(0));
        let actual = validate_bucket_ownership_index_from_entries(1, shard, &entries, 0, u32::MAX);
        let indexed = OWNERSHIP_INDEX_BLOCK_VISITS.with(|visits| visits.get());
        let (expected, scanned) = scan_validation(shard, &entries, 0, u32::MAX);
        assert_eq!(actual.missing_owner_block_refs, 0);
        assert_eq!(actual.mismatches, expected.mismatches);
        assert_eq!(indexed, count);
        assert_eq!(scanned, count * (count + 1) / 2);
        println!("ownership blocks={count} indexed_visits={indexed} scan_visits={scanned}");
    }
}

#[test]
fn ownership_validation_preserves_missing_and_misfiled_reports() {
    let (_dir, engine) = fixture(32);
    let mut shards = engine.shards.write().unwrap();
    let shard = shards.get_mut(&1).unwrap();
    let mut entries = collect_live_block_entries(shard);
    entries[0].filing_is_known = false;
    entries[1].filed_routing_bucket = entries[1].filed_routing_bucket.wrapping_add(1);
    let held = entries[2].address.clone();
    for address in [
        ElementEntry::from_parts(
            held.block_slab_id() + 1,
            held.offset(),
            held.length(),
            held.block_id(),
            None,
        ),
        ElementEntry::from_parts(
            held.block_slab_id(),
            held.offset() + 1,
            held.length(),
            held.block_id(),
            None,
        ),
        ElementEntry::from_parts(
            held.block_slab_id(),
            held.offset(),
            held.length() + 1,
            held.block_id(),
            None,
        ),
        ElementEntry::from_parts(
            held.block_slab_id(),
            held.offset(),
            held.length(),
            None,
            None,
        ),
    ] {
        let mut absent = entries[2].clone();
        absent.address = address;
        entries.push(absent);
    }
    let mut wrong_kind = entries[2].clone();
    wrong_kind.kind = stored_model_kind("string");
    entries.push(wrong_kind);
    assert_same(shard, &entries, 0, u32::MAX);
    assert_same(shard, &entries, 0, 31);
    let owner = expected_live_block_object_id(1, &entries[2]);
    let bucket_id = entries[2].filed_bucket().unwrap();
    shard
        .bucket_index
        .bucket_map
        .get_mut(&bucket_id)
        .unwrap()
        .object_index
        .remove(&owner);
    assert_same(shard, &entries, 0, u32::MAX);
    OWNERSHIP_INDEX_BLOCK_VISITS.with(|visits| visits.set(0));
    assert_same(shard, &[], 0, u32::MAX);
    assert_eq!(OWNERSHIP_INDEX_BLOCK_VISITS.with(|visits| visits.get()), 0);
}
