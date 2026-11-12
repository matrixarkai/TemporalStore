// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[path = "bucket_summary_entries.rs"]
mod captured_entries;
use captured_entries::CapturedEntries;

/// An owned, short-lived capture. It holds neither a shard guard nor mutable engine data.
pub(super) struct BucketSummarySnapshot {
    shard_id: ShardId,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    metadata: BucketSummaryMetadata,
    entries: CapturedEntries,
    with_digests: bool,
    dump_sequence: Option<BucketDumpSequenceAnnotation>,
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
            start_routing_bucket,
            end_routing_bucket,
            metadata,
            entries,
            with_digests,
            dump_sequence,
        } = self;
        // Identity resolution, folding and hashing use only immutable captured values.
        // No engine lock or full dump index image is available here.
        let entries = entries.finish();
        let summaries =
            metadata.summaries_from_entries(start_routing_bucket, end_routing_bucket, &entries);
        let digests = if with_digests {
            bucket_generation_digests_from_entries(shard_id, &entries)
        } else {
            BTreeMap::new()
        };
        drop(entries);
        let summaries = match dump_sequence {
            Some(annotation) => annotation.apply(summaries),
            None => summaries,
        };
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
        let slot_refs = shard.bucket_index.objects.values().flat_map(|table| {
            table
                .iter()
                .filter_map(|(key, slot)| slot.refs().map(|refs| (Arc::clone(key), refs.clone())))
        });
        let lookup_refs = shard
            .bucket_index
            .object_block_lookup
            .iter()
            .map(|(_, key, refs)| (Arc::clone(key), refs.clone()));
        let entries = CapturedEntries::capture(shard, slot_refs.chain(lookup_refs));
        let metadata = BucketSummaryMetadata::capture(shard);
        // Keep manifest selection inside the capture's read guard, as before. A newer dump
        // published while finish() runs must not be paired with the older block snapshot.
        let dump_sequence = latest_bucket_dump_manifest_shared_at(&self.index_dir, shard_id)
            .map(|manifest| BucketDumpSequenceAnnotation::capture(&manifest));
        Some(BucketSummarySnapshot {
            shard_id,
            start_routing_bucket: start,
            end_routing_bucket: end,
            metadata,
            entries,
            with_digests,
            dump_sequence,
        })
    }
}

#[cfg(test)]
#[path = "bucket_summary_snapshot_tests.rs"]
mod tests;
