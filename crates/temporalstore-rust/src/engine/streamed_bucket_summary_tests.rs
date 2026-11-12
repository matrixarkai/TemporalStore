// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn seeded(dir: &std::path::Path, count: usize, end: u32) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        32 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("index"),
    );
    let loaded = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: end,
        readonly: false,
        table_name: String::new(),
    });
    assert!(loaded.status.ok, "{:?}", loaded.status);
    for start in (0..count).step_by(128) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: (start..count.min(start + 128))
                .map(|index| Command::StringSet {
                    key: format!("streamed-{index:08}"),
                    value: vec![b'x'; 128],
                })
                .collect(),
        });
        assert!(response.status.ok, "{:?}", response.status);
        assert!(response.responses.iter().all(|row| row.status.ok));
    }
    engine
}

fn assert_same(shard: &ShardState, end: u32) {
    let entries = collect_live_block_entries(shard);
    let expected = bucket_storage_summaries_from_entries(shard, 0, end, &entries);
    let (actual, none) = bucket_summaries_and_digests(1, shard, 0, end, false);
    assert_eq!(actual, expected);
    assert!(none.is_empty());
    assert_eq!(
        actual.iter().map(|row| row.block_ref_count).sum::<u64>(),
        entries.len() as u64
    );
    assert_eq!(
        actual.iter().map(|row| row.physical_bytes).sum::<u64>(),
        entries.iter().map(|row| row.address.length()).sum::<u64>()
    );
    let (with_digests, digests) = bucket_summaries_and_digests(1, shard, 0, end, true);
    assert_eq!(actual, with_digests);
    assert!(!digests.is_empty());
}

#[test]
fn streamed_bucket_summaries_preserve_updated_and_deleted_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = seeded(dir.path(), 256, u32::MAX);
    for command in [
        Command::HashMultiSet {
            key: "hash".into(),
            entries: (0..24)
                .map(|i| (format!("field-{i}"), vec![b'h'; 128]))
                .collect(),
        },
        Command::SetAdd {
            key: "set".into(),
            member: b"member".to_vec(),
        },
        Command::StringSet {
            key: "streamed-00000001".into(),
            value: vec![b'y'; 512],
        },
        Command::StringDelete {
            key: "streamed-00000002".into(),
        },
        Command::HashDelete {
            key: "hash".into(),
            field: "field-3".into(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
    let shards = engine.shards.read().unwrap();
    assert_same(shards.get(&1).unwrap(), u32::MAX);
}

#[test]
fn streamed_bucket_summaries_preserve_released_buckets_and_narrow_routing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = seeded(dir.path(), 256, 127);
    let buckets = {
        let shards = engine.shards.read().unwrap();
        shards
            .get(&1)
            .unwrap()
            .bucket_index
            .bucket_map
            .keys()
            .copied()
            .take(8)
            .collect::<Vec<_>>()
    };
    let manifest = engine
        .create_bucket_dump_manifest(1, buckets.clone())
        .unwrap();
    engine.clear_dumped_bucket_dirty_state(1, &manifest);
    let released = engine.release_bucket_index_blocks(1, buckets);
    assert!(released.0 > 0 && released.1 > 0, "{released:?}");
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    assert!(!shard.bucket_index.released_buckets.is_empty());
    assert_same(shard, 127);
}

#[test]
fn streamed_bucket_summaries_preserve_the_model_map_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = seeded(dir.path(), 128, 63);
    let shards = engine.shards.read().unwrap();
    let mut shard = shards.get(&1).unwrap().clone();
    shard.bucket_index.bucket_map.clear();
    assert!(!collect_live_block_entries(&shard).is_empty());
    assert_same(&shard, 63);
}

#[test]
#[cfg(feature = "alloc-probe")]
fn streamed_bucket_summaries_avoid_the_resident_entry_vector_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = seeded(dir.path(), 4096, 255);
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    assert!(shard.bucket_index.released_buckets.is_empty());
    let retained_work = || {
        let entries = collect_live_block_entries(shard);
        let expected = bucket_storage_summaries_from_entries(shard, 0, 255, &entries);
        (entries, expected)
    };
    let streamed_work = || bucket_summaries_and_digests(1, shard, 0, 255, false);
    drop(retained_work());
    drop(streamed_work());
    let probe = crate::alloc_probe::Probe::start();
    let (entries, expected) = retained_work();
    let retained = probe.stop();
    assert!(entries.len() >= 4096);
    let entry_bytes = entries.len() * std::mem::size_of::<LiveBlockEntry>();
    drop(entries);
    let probe = crate::alloc_probe::Probe::start();
    let (actual, none) = streamed_work();
    let streamed = probe.stop();
    assert_eq!(actual, expected);
    assert!(none.is_empty());
    eprintln!("streamed_bucket_summaries rows=4096 retained_alloc_bytes={} streamed_alloc_bytes={} entry_payload_bytes={entry_bytes}", retained.alloc_bytes, streamed.alloc_bytes);
    assert!(
        retained.alloc_bytes >= streamed.alloc_bytes + entry_bytes as u64,
        "retained={retained:?} streamed={streamed:?} entry_bytes={entry_bytes}"
    );
    let mut model_only = shard.clone();
    model_only.bucket_index.bucket_map.clear();
    let direct_work =
        || super::super::storage_bucket_internals::collect_model_live_block_entries(&model_only);
    let collecting_work = || collect_live_block_entries(&model_only);
    drop(direct_work());
    drop(collecting_work());
    let probe = crate::alloc_probe::Probe::start();
    let direct_entries = direct_work();
    let direct = probe.stop();
    let probe = crate::alloc_probe::Probe::start();
    let collected = collecting_work();
    let collecting = probe.stop();
    assert_eq!(collected.len(), direct_entries.len());
    assert!(collected.len() >= 4096);
    // Permit fixed bookkeeping, not another per-entry vector.
    assert!(
        collecting.alloc_bytes <= direct.alloc_bytes + 4096,
        "direct={direct:?} collecting={collecting:?}"
    );
}
