// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::block_store::BlockStoreBandState;
use crate::engine::golden::{
    native_api_golden_corpus_report, native_feature_sequence_golden_corpus_report,
};
use crate::types::{
    ContextAuditRef, ContextChildRef, ContextCompressionEvent, ContextEmbedding,
    ContextExtractedEventIndexes, ContextSummary, ContextWire, FeatureFilter, FeatureFilterOp,
    ReplicatedCommand,
};
use crate::{BlockAddress, BlockStoreOptions, LocalBlockStore};

fn wait_for_fresh_admission_second() {
    loop {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch");
        if elapsed.subsec_millis() < 100 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn assert_cache_latency_histograms_observed(stats: matrixcache::CacheStats) {
    assert!(stats.read_through_latency_samples > 0);
    assert!(stats.refill_latency_samples > 0);
    assert!(stats.writeback_latency_samples > 0);
    assert!(stats.eviction_latency_samples > 0);
    assert!(stats.compaction_latency_samples > 0);
    assert_eq!(
        stats.read_through_latency_samples,
        stats.read_through_latency_le_10us
            + stats.read_through_latency_le_100us
            + stats.read_through_latency_le_1ms
            + stats.read_through_latency_le_10ms
            + stats.read_through_latency_gt_10ms
    );
    assert_eq!(
        stats.refill_latency_samples,
        stats.refill_latency_le_10us
            + stats.refill_latency_le_100us
            + stats.refill_latency_le_1ms
            + stats.refill_latency_le_10ms
            + stats.refill_latency_gt_10ms
    );
    assert_eq!(
        stats.writeback_latency_samples,
        stats.writeback_latency_le_10us
            + stats.writeback_latency_le_100us
            + stats.writeback_latency_le_1ms
            + stats.writeback_latency_le_10ms
            + stats.writeback_latency_gt_10ms
    );
    assert_eq!(
        stats.eviction_latency_samples,
        stats.eviction_latency_le_10us
            + stats.eviction_latency_le_100us
            + stats.eviction_latency_le_1ms
            + stats.eviction_latency_le_10ms
            + stats.eviction_latency_gt_10ms
    );
    assert_eq!(
        stats.compaction_latency_samples,
        stats.compaction_latency_le_10us
            + stats.compaction_latency_le_100us
            + stats.compaction_latency_le_1ms
            + stats.compaction_latency_le_10ms
            + stats.compaction_latency_gt_10ms
    );
}


mod conformance;
mod concurrent_commit;
mod phase1_flat;
mod part1;
mod part2;
mod part3;
mod part4;
