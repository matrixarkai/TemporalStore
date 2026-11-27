// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Retirement of covered, physically empty owners in source-qualified images.
use super::*;

struct Proposal {
    bucket: u32,
    generation: u64,
    wal: u64,
    index: u64,
    dirty: bool,
    dirty_keys: u64,
    deleted: BTreeSet<u64>,
}

#[derive(Default)]
pub(super) struct Prepared {
    pub(super) ids: BTreeSet<u32>,
    source: Option<(u32, u32)>,
    proposals: Vec<Proposal>,
}

fn source_qualified(image: &mut ShardState) -> bool {
    let Some(source) = image.source_owner_range else { return false; };
    if image.routing_range_known && image.routing_range() != source {
        return false;
    }
    // This is persisted image authority, never a requested/target range guess.
    image.set_routing_range(source.0, source.1);
    image.checkpoint_wal_sequence().is_some_and(|anchor| anchor > 0)
}

fn physical_candidate(image: &ShardState, bucket: u32, node: &BucketNode) -> bool {
    node.routing_bucket == bucket
        && node.deleted()
        && !node.loading()
        && node.dirty_generation != u64::MAX
        && node.block_index.is_empty()
        && node.tombstone_elements.is_empty()
        && !image.bucket_index.released_buckets.contains(&bucket)
        && !node.deleted_object_index.is_empty()
        && node.object_index.iter().count() == node.deleted_object_index.iter().count()
}

fn prepare(image: &mut ShardState, selected: &BTreeSet<u32>, dirty_only: bool) -> Prepared {
    if selected.is_empty() || !source_qualified(image) {
        return Prepared::default();
    }
    let mut proposals = Vec::new();
    for &bucket in selected {
        let Some(node) = image.bucket_index.bucket_map.get(&bucket) else { continue; };
        if !physical_candidate(image, bucket, node) || (dirty_only && !node.dirty()) {
            continue;
        }
        let deleted: BTreeSet<_> = node.deleted_object_index.iter().copied().collect();
        if !node.object_index.iter().all(|id| deleted.contains(id)) {
            continue;
        }
        proposals.push(Proposal {
            bucket, generation: node.dirty_generation,
            wal: node.first_dirty_wal_sequence, index: node.first_dirty_index_log_sequence,
            dirty: node.dirty(), dirty_keys: image.dirty_objects.bucket_count(bucket),
            deleted,
        });
    }
    let candidates = proposals.iter().map(|proposal| proposal.bucket).collect();
    let eligible = selected_owner_absence::selected_buckets_without_logical_authority(image, &candidates);
    proposals.retain(|proposal| eligible.contains(&proposal.bucket));
    Prepared { ids: eligible, source: image.source_owner_range, proposals }
}

impl Prepared {
    pub(super) fn append_empty_coverage(&self, summaries: &mut Vec<BucketStorageSummary>) {
        for proposal in &self.proposals {
            // Keep selection explicit even when every selected owner was removed.
            summaries.push(BucketStorageSummary {
                routing_bucket: proposal.bucket,
                dirty_generation: proposal.generation.saturating_add(proposal.dirty_keys),
                ..BucketStorageSummary::default()
            });
        }
    }

    /// Caller owns the exact captured owner and has passed its successful-completion fence.
    pub(super) fn retire_matching(&self, image: &mut ShardState) -> Vec<BucketNode> {
        if self.ids.is_empty() || image.source_owner_range != self.source
            || !image.routing_range_known || Some(image.routing_range()) != self.source
        {
            return Vec::new();
        }
        let mut matched = Vec::new();
        for proposal in &self.proposals {
            let Some(node) = image.bucket_index.bucket_map.get(&proposal.bucket) else { continue; };
            if physical_candidate(image, proposal.bucket, node)
                && node.dirty() == proposal.dirty
                && node.dirty_generation == proposal.generation
                && node.first_dirty_wal_sequence == proposal.wal
                && node.first_dirty_index_log_sequence == proposal.index
                && image.dirty_objects.bucket_count(proposal.bucket) == proposal.dirty_keys
                && node.deleted_object_index.iter().count() == proposal.deleted.len()
                && node.deleted_object_index.iter().all(|id| proposal.deleted.contains(id))
                && node.object_index.iter().all(|id| proposal.deleted.contains(id))
            {
                matched.push(proposal.bucket);
            }
        }
        image.dirty_objects.drain_buckets(&matched);
        let mut retired = Vec::with_capacity(matched.len());
        for bucket in matched {
            if let Some(node) = image.bucket_index.bucket_map.remove(&bucket) {
                retired.push(node);
            }
            image.buckets_pending_flag_refresh.remove(&bucket);
            image.bucket_recency.remove(&bucket);
            if let Some(owners) = image.bucket_index.bucket_owners.get_mut() {
                owners.forget_bucket(bucket);
            }
        }
        retired
    }
}

pub(super) fn normalize_selected(image: &mut ShardState, selected: &BTreeSet<u32>) -> Prepared {
    let prepared = prepare(image, selected, true);
    drop(prepared.retire_matching(image));
    prepared
}

/// Only the selected, verified covered image; never call after logical suffix replay.
pub(super) fn reconcile_covered_image(image: &mut ShardState) {
    if image.source_owner_range.is_none() {
        return;
    }
    let selected = image.bucket_index.bucket_map.iter().map(|(&bucket, _)| bucket).collect();
    let prepared = prepare(image, &selected, false);
    drop(prepared.retire_matching(image));
}
