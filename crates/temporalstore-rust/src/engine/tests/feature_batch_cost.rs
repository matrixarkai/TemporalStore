// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::alloc_probe::ClassSpan;
use crate::types::{BatchHashMultiSetValue, BatchHashTimestampSetRequest, BatchHashTimestampSetRow};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct EventTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

fn body(start: usize, generation: i64) -> Vec<u8> {
    let fields: Vec<_> = (0..20).map(|field| format!("stats:field-{field:02}")).chain(
        std::iter::once("_ts:stats".to_string())).collect();
    let timestamp = EventTimestamp { seconds: generation, nanos: 0 }.encode_to_vec();
    let rows = (start..start + 512).map(|row| {
        let mut values = Vec::new();
        for field in 0..20 {
            let mut state = (row as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15)
                ^ (field as u64 + 1).wrapping_mul(0xd1b54a32d192ed03) ^ generation as u64;
            let bytes = (0..48).map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            }).collect();
            values.push(BatchHashMultiSetValue(bytes));
        }
        values.push(BatchHashMultiSetValue(timestamp.clone()));
        BatchHashTimestampSetRow(format!("row-{row:08}"), generation, 0, values)
    }).collect();
    rmp_serde::to_vec_named(&BatchHashTimestampSetRequest {
        shard_id: 1, fields, timestamp_field: "_ts:stats".into(), ttl_ms: None, rows,
    }).unwrap()
}

#[test]
#[ignore = "bounded allocation and WAL-read measurement"]
fn measure_feature_batch_phases_under_cache_pressure() {
    for capacity in [16 * 1024 * 1024, 64 * 1024] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
            capacity, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
            crate::block_store::BlockStoreOptions::default(), false,
        );
        assert!(engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1, load_version: 1, local_node_id: None, shard_uri: String::new(),
            start_routing_bucket: 0, end_routing_bucket: 1023, readonly: false, table_name: "feature-cost".into(),
        }).status.ok);
        assert!(engine.set_config(crate::control::SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: false, ..Config::default() },
        }).ok);
        for (phase, generation) in [("initial", 1), ("overwrite", 2), ("stale", 1)] {
            for batch in 0..4 {
                let encoded = body(batch * 512, generation);
                let request: BorrowedHashTimestampRequest<'_> = rmp_serde::from_slice(&encoded).unwrap();
                let wal_before = engine.write_ahead_log_store().stats_without_rescan(1);
                let cache_before = engine.cache.stats();
                let span = ClassSpan::open();
                let begin = std::time::Instant::now();
                let (result, keys) = engine.batch_execute_borrowed_hash_rows(request);
                let seconds = begin.elapsed().as_secs_f64();
                let measured = span.close().unwrap();
                assert!(result.status.ok && result.errors.is_empty());
                assert_eq!(result.applied, if phase == "stale" { 0 } else { 512 });
                assert_eq!(result.skipped, if phase == "stale" { 512 } else { 0 });
                assert_eq!(keys.len(), result.applied);
                let wal_after = engine.write_ahead_log_store().stats_without_rescan(1);
                let cache_after = engine.cache.stats();
                assert!(cache_after.memory_bytes <= capacity as u64);
                println!("FEATURE_COST cache={capacity} phase={phase} batch={batch} rows=512 seconds={seconds:.6} allocs={} bytes={} wal_read={} memory={} cache_before={:?} cache_after={:?}",
                    measured.total.allocs, measured.total.alloc_bytes,
                    wal_after.bytes_read - wal_before.bytes_read, cache_after.memory_bytes,
                    cache_before, cache_after);
                println!("FEATURE_CLASSES cache={capacity} phase={phase} batch={batch} {} residual_calls={} residual_bytes={}",
                    measured.classes.report_line(), measured.residual_allocs(), measured.residual_bytes());
            }
        }
    }
}
