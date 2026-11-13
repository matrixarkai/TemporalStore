// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::block_store::ElementEntry;
use crate::engine::timestamped_series::TimestampedSeries as _;
use crate::wal::WalOutcomeItem;

const KINDS: [&str; 6] = [
    "feature",
    "context_index",
    "context_audit",
    "context_child",
    "context_summary",
    "context_compression",
];

fn item(kind: &str, key: &str, at: Option<u64>, offset: Option<u64>) -> WalOutcomeItem {
    WalOutcomeItem {
        kind: kind.into(),
        object_key: key.into(),
        element_name: at.map(|at| at.to_string()),
        object_id: stable_block_object_id(1, kind, key),
        routing_bucket: 0,
        address: offset.map(|offset| ElementEntry::from_parts(1, offset, 16, None, None)),
        value: None,
        ttl: None,
        deleted: offset.is_none(),
        meta: false,
    }
}

fn addresses(shard: &ShardState, kind: &str, key: &str) -> Vec<ElementEntry> {
    macro_rules! values {
        ($map:expr) => {
            $map.get(key)
                .map(|series| series.addresses().cloned().collect())
                .unwrap_or_default()
        };
    }
    match kind {
        "feature" => values!(shard.features()),
        "context_index" => values!(shard.context_indexes()),
        "context_audit" => values!(shard.context_audits()),
        "context_child" => values!(shard.context_children()),
        "context_summary" => values!(shard.context_summaries()),
        "context_compression" => values!(shard.context_compressions()),
        _ => panic!("unexpected kind"),
    }
}

#[test]
fn new_timestamp_outcomes_publish_only_the_inserted_block_for_all_series_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for (index, kind) in KINDS.iter().enumerate() {
        let key = format!("series-{kind}");
        let first = item(kind, &key, Some(10), Some(index as u64 * 100));
        let next = item(kind, &key, Some(20), Some(index as u64 * 100 + 32));
        assert!(engine.apply_outcome_item(1, &first, &[]));
        let (applied, builds) = super::super::state::measure_block_identity_builds_for_test(|| {
            engine.apply_outcome_item(1, &next, &[])
        });
        assert!(applied);
        assert_eq!(
            builds, 0,
            "{kind}: new instants must not invert the shard's identities"
        );
        let shards = engine.shards.read().unwrap();
        let shard = &shards[&1];
        assert_eq!(
            addresses(shard, kind, &key),
            vec![first.address.unwrap(), next.address.unwrap()]
        );
        assert_eq!(
            shard
                .bucket_index
                .object_block_refs(kind, &key)
                .unwrap()
                .len(),
            2
        );
    }
}

#[test]
fn timestamp_replacements_and_removals_keep_the_full_reconciliation_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for (index, kind) in KINDS.iter().enumerate() {
        let key = format!("series-{kind}");
        let base = index as u64 * 1000;
        for (at, offset) in [(10, base), (20, base + 32), (30, base + 64)] {
            assert!(engine.apply_outcome_item(1, &item(kind, &key, Some(at), Some(offset)), &[]));
        }
        let replacement = item(kind, &key, Some(20), Some(base + 128));
        let (applied, builds) = super::super::state::measure_block_identity_builds_for_test(|| {
            engine.apply_outcome_item(1, &replacement, &[])
        });
        assert!(applied && builds > 0);
        {
            let shards = engine.shards.read().unwrap();
            let held = addresses(&shards[&1], kind, &key);
            assert_eq!(held.len(), 3);
            assert_eq!(held[1], replacement.address.unwrap());
        }
        assert!(engine.apply_outcome_item(1, &item(kind, &key, Some(10), None), &[]));
        {
            let shards = engine.shards.read().unwrap();
            assert_eq!(addresses(&shards[&1], kind, &key).len(), 2);
            assert_eq!(
                shards[&1]
                    .bucket_index
                    .object_block_refs(kind, &key)
                    .unwrap()
                    .len(),
                2
            );
        }
        assert!(engine.apply_outcome_item(1, &item(kind, &key, None, None), &[]));
        assert!(addresses(&engine.shards.read().unwrap()[&1], kind, &key).is_empty());
    }
}
