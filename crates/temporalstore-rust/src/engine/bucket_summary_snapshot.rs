// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

/// An owned, short-lived capture. It holds neither a shard guard nor mutable engine data.
pub(super) struct BucketSummarySnapshot {
    shard_id: ShardId,
    summaries: Vec<BucketStorageSummary>,
    digest_entries: Option<Vec<LiveBlockEntry>>,
}

impl BucketSummarySnapshot {
    pub(super) fn finish(
        self,
    ) -> (
        Vec<BucketStorageSummary>,
        BTreeMap<u32, BucketFingerprintDigest>,
    ) {
        let Self {
            shard_id,
            summaries,
            digest_entries,
        } = self;
        // Hashing, sorting and deduplication use only immutable captured values. No engine
        // lock is available here, and no dump index image is kept alive by this capture.
        let digests = digest_entries
            .map(|entries| bucket_generation_digests_from_entries(shard_id, &entries))
            .unwrap_or_default();
        (summaries, digests)
    }
}

impl TemporalEngine {
    pub(super) fn capture_bucket_storage_summaries(
        &self,
        shard_id: ShardId,
        with_digests: bool,
    ) -> Option<BucketSummarySnapshot> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id)?;
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        let (summaries, digest_entries) = if with_digests {
            let entries = collect_live_block_entries(shard);
            let summaries = bucket_storage_summaries_from_entries(shard, start, end, &entries);
            (summaries, Some(entries))
        } else {
            (
                bucket_summaries_and_digests(shard_id, shard, start, end, false).0,
                None,
            )
        };
        // Keep manifest selection inside the capture's read guard, as before. A newer dump
        // published while finish() runs must not be paired with the older block snapshot.
        let summaries = match latest_bucket_dump_manifest_shared_at(&self.index_dir, shard_id) {
            Some(manifest) => merge_last_dump_sequence(summaries, &manifest),
            None => summaries,
        };
        Some(BucketSummarySnapshot {
            shard_id,
            summaries,
            digest_entries,
        })
    }
}

#[cfg(test)]
#[path = "bucket_summary_snapshot_tests.rs"]
mod tests;
