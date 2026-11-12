// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn one_token_bucket_take_stages_one_exact_result_even_when_denied() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let mut state = ShardState::default();
    state.set_routing_range(17, 1040);
    for _ in 0..3 {
        crate::engine::block_in_wal::begin_write();
        let result = execute_on_shard(
            &engine.cache,
            &engine.block_store,
            0,
            true,
            false,
            false,
            0,
            false,
            false,
            false,
            1,
            17,
            1040,
            &mut state,
            Command::BucketTake {
                key: "quota".into(),
                tokens: 3.0,
                capacity: 5.0,
                refill_per_sec: 0.0,
            },
        );
        assert!(result.mutated);
        let outcomes = crate::engine::block_in_wal::take_outcomes();
        assert_eq!(
            outcomes.len(),
            1,
            "one result must not be staged repeatedly"
        );
        let item = &outcomes[0];
        assert_eq!(item.kind, "bucket");
        assert_eq!(item.object_key, "quota");
        assert!(item.meta && !item.deleted && item.address.is_none());
        assert_eq!(item.routing_bucket, block_routing_bucket("quota", 17, 1040));
        let bytes = item.value.as_ref().unwrap();
        assert_eq!(bytes.len(), 16);
        let written = (
            f64::from_le_bytes(bytes[..8].try_into().unwrap()),
            u64::from_le_bytes(bytes[8..].try_into().unwrap()),
        );
        assert_eq!(state.buckets.get("quota"), Some(&written));
        assert_eq!(written.0, 2.0);
        assert!(crate::engine::block_in_wal::take_staged().is_empty());
    }
}
