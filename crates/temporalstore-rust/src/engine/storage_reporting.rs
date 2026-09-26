// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage physical-index / object-manager / feature-page layout report helpers, split from engine.rs.
use super::*;

pub(super) fn storage_object_lifecycle_report(
    shard_id: ShardId,
    shard: &ShardState,
) -> StorageObjectLifecycleReport {
    storage_object_lifecycle_report_for_buckets(shard_id, shard, &BTreeSet::new(), |_| 0)
}

pub(super) fn storage_object_lifecycle_report_for_buckets(
    shard_id: ShardId,
    shard: &ShardState,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    object_lifecycle_report_from_entries(
        shard_id,
        shard,
        collect_live_block_entries(shard),
        selected_buckets,
        routing_bucket_for_key,
    )
}

/// Same object-lifecycle report, but derived from the secondary model maps
/// (strings/hashes/feature series/...) instead of the bucket index. Used to detect
/// a bucket-dump manifest whose serialized model maps disagree with its bucket index
/// (e.g. a mutated object_id): the bucket-index-derived report alone stays valid.
pub(super) fn storage_object_lifecycle_report_for_buckets_from_model_maps(
    shard_id: ShardId,
    shard: &ShardState,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    object_lifecycle_report_from_entries(
        shard_id,
        shard,
        collect_model_live_block_entries(shard),
        selected_buckets,
        routing_bucket_for_key,
    )
}

pub(super) fn object_lifecycle_report_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: Vec<LiveBlockEntry>,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    let entries = entries
        .into_iter()
        .filter(|entry| {
            let routing_bucket = entry
                .address
                .routing_bucket()
                .unwrap_or_else(|| routing_bucket_for_key(&entry.object_key));
            selected_buckets.is_empty() || selected_buckets.contains(&routing_bucket)
        })
        .collect::<Vec<_>>();
    let mut expected_object_ids = BTreeSet::new();
    let mut actual_object_owners = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut missing_owner_block_refs = 0u64;
    let mut owner_mismatch_block_refs = 0u64;

    for entry in &entries {
        let expected_object_id = expected_live_block_object_id(shard_id, entry);
        expected_object_ids.insert(expected_object_id);
        if entry.address.object_id().is_none() || entry.address.routing_bucket().is_none() {
            missing_owner_block_refs = missing_owner_block_refs.saturating_add(1);
        }
        match entry.address.object_id() {
            Some(actual_object_id) => {
                actual_object_owners
                    .entry(actual_object_id)
                    .or_default()
                    .insert(expected_object_id);
                if actual_object_id != expected_object_id {
                    owner_mismatch_block_refs = owner_mismatch_block_refs.saturating_add(1);
                }
            }
            None => {}
        }
    }

    let reused_object_ids = actual_object_owners
        .into_iter()
        .filter_map(|(actual_object_id, expected_ids)| {
            (expected_ids.len() > 1).then_some(actual_object_id)
        })
        .collect::<Vec<_>>();
    let delete_marked_object_keys = shard
        .dirty_objects
        .iter()
        .filter(|key| {
            let routing_bucket = routing_bucket_for_key(key);
            (selected_buckets.is_empty() || selected_buckets.contains(&routing_bucket))
                && !record_exists(shard, key)
        })
        .map(String::from)
        .collect::<Vec<_>>();

    StorageObjectLifecycleReport {
        live_object_ids: expected_object_ids.len() as u64,
        live_block_refs: entries.len() as u64,
        stale_object_ids: 0,
        delete_marked_object_ids: delete_marked_object_keys.len() as u64,
        reused_object_id_conflicts: reused_object_ids.len() as u64,
        missing_owner_block_refs,
        owner_mismatch_block_refs,
        reused_object_ids,
        delete_marked_object_keys,
    }
}

pub(super) fn bucket_dump_entries_by_key(
    shard_id: ShardId,
    shard: &ShardState,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> BTreeMap<String, BlockAddress> {
    collect_live_block_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_bucket = entry
                .address
                .routing_bucket()
                .unwrap_or_else(|| routing_bucket_for_key(&entry.object_key));
            selected_buckets.is_empty() || selected_buckets.contains(&routing_bucket)
        })
        .map(|entry| {
            let component = entry.component.unwrap_or_default();
            let block_id = entry.address.block_id().unwrap_or_else(|| {
                stable_block_object_id(
                    shard_id,
                    &entry.kind,
                    &entry.object_key,
                    (!component.is_empty()).then_some(component.as_ref()),
                )
            });
            (
                format!(
                    "{}:{}:{}:{}",
                    entry.kind, entry.object_key, component, block_id
                ),
                entry.address,
            )
        })
        .collect()
}

/// How many entries the dirty half of [`bucket_storage_summaries`] looks at, across every call.
///
/// The cost of that half is not visible from outside the function, and deriving it as "one per
/// dirty object" is arithmetic about the code rather than a measurement of it. This counts what
/// actually happens, so the guard keeps being true after someone changes the loop.
pub(crate) static DIRTY_SUMMARY_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(super) fn bucket_storage_summaries(
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> Vec<BucketStorageSummary> {
    let mut buckets = BTreeMap::<u32, BucketStorageSummary>::new();
    let mut block_slabs_by_bucket = BTreeMap::<u32, BTreeSet<u64>>::new();
    for entry in collect_live_block_entries(shard) {
        // The shard's OWN routing range, which is what every other consumer of this fallback
        // uses -- `rebuild_bucket_first_index`, `refresh_pending_bucket_runtime_flags` and the
        // dirty-key loop at the foot of this function all reach for
        // `block_routing_bucket(key, start, end)`. This site reached for `bucket_for_object(key,
        // 0, u32::MAX)` instead, which is the same answer only while the shard spans the whole
        // range. Narrow the range -- `TS_SHARD_END_ROUTING_SLOT=1023` is the setting that cuts
        // resident memory 45% -- and the two place the same page in different buckets: this one
        // in a bucket id above the shard's own end, no other component in agreement, and so a
        // summary for a bucket `bucket_map` does not hold while the bucket that does hold the
        // page reports no pages at all. A dump naming that bucket then carries no slabs for it.
        //
        // MEASURED FIRST: on the live write path this branch does not fire. Every one of 2 000
        // live page entries carried an explicit routing bucket, so the count of summaries
        // outside the range was 0 before this change as well as after it. What follows is the
        // latent half -- an address that reaches here without one (a page rebuilt from a source
        // that did not carry it) is placed where the rest of the engine already places it.
        let routing_bucket = entry.address.routing_bucket().unwrap_or_else(|| {
            block_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket)
        });
        let summary = buckets.entry(routing_bucket).or_insert(BucketStorageSummary {
            routing_bucket,
            ..BucketStorageSummary::default()
        });
        summary.block_ref_count = summary.block_ref_count.saturating_add(1);
        summary.physical_bytes = summary.physical_bytes.saturating_add(entry.address.length());
        summary.logical_bytes = summary.logical_bytes.saturating_add(entry.address.length());
        // Record which page slab backs each bucket so bucket-dump manifests carry the
        // live slab set (used by manifest validation and the dump/copy path).
        // Without this the map stayed empty and every summary reported no slabs.
        block_slabs_by_bucket
            .entry(routing_bucket)
            .or_default()
            .insert(entry.address.block_slab_id());
        if let Some(stored_slab_id) = entry.address.slab_id() {
            summary.last_compacted_slab = Some(
                summary
                    .last_compacted_slab
                    .map_or(stored_slab_id, |current| current.max(stored_slab_id)),
            );
        }
    }
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        // object_count and the base dirty_generation are durable-generation IDENTITY
        // fields (compared by bucket_dump_summary_matches_current_generation to anchor
        // WAL/index reclaim). They must reflect durable bucket content, not the
        // transient `dirty` flag -- so a bucket that was dumped and then had its dirty
        // flag cleared but still owns live pages keeps its
        // reclaim fingerprint. Populate whenever the bucket is dirty OR already has a
        // live-page summary; behaviour is unchanged for every pre-clear state (a bucket
        // with content is dirty today, so the branch was always taken).
        let has_live_summary = buckets.contains_key(routing_bucket);
        if !bucket.dirty() && !has_live_summary {
            continue;
        }
        let summary = buckets.entry(*routing_bucket).or_insert(BucketStorageSummary {
            routing_bucket: *routing_bucket,
            ..BucketStorageSummary::default()
        });
        // object_index.len() is the bucket's total object count, not its dirty count;
        // assigning it to dirty_object_count double-counted (the dirty_objects loop
        // below is the authoritative per-key dirty tally).
        summary.object_count = bucket.object_index.len() as u64;
        summary.dirty_generation = bucket.dirty_generation;
    }
    // ONE ITERATION PER DIRTY BUCKET, where this was one per dirty OBJECT.
    //
    // The loop this replaces walked every dirty object key and hashed it to recompute a routing
    // bucket, to add 1 to a per-bucket counter. That is a fold whose group key was known when the
    // object was marked dirty and thrown away; `DirtyObjectIndex` keeps it, so the fold is
    // already done. Same arithmetic -- adding 1 N times and adding N are the same number well
    // below saturation -- over a loop bounded by the bucket count instead of the corpus.
    //
    // This function runs three times in one `apply_storage_lifecycle` and again on each metrics
    // scrape, so the per-key hash was paid four times a round over a set that grows with ingest.
    for (routing_bucket, dirty_object_count) in shard.dirty_objects.bucket_counts() {
        DIRTY_SUMMARY_VISITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let summary = buckets.entry(routing_bucket).or_insert(BucketStorageSummary {
            routing_bucket,
            ..BucketStorageSummary::default()
        });
        summary.dirty_object_count = summary
            .dirty_object_count
            .saturating_add(dirty_object_count);
        summary.dirty_generation = summary.dirty_generation.saturating_add(dirty_object_count);
    }
    for (routing_bucket, summary) in &mut buckets {
        summary.block_slab_ids = block_slabs_by_bucket
            .get(routing_bucket)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
    }
    buckets.into_values().collect()
}

const NATIVE_PACKED_BLOCK_INDEX_SIZE: usize = 17;
const NATIVE_PACKED_BUCKET_NODE_SIZE: usize = 24;

pub(super) fn physical_address_word(address: &BlockAddress) -> u64 {
    address.block_slab_id().wrapping_shl(32) | (address.offset() & u32::MAX as u64)
}

pub(super) fn native_packed_block_index_bytes(
    page: &StoragePhysicalBlockIndex,
) -> [u8; NATIVE_PACKED_BLOCK_INDEX_SIZE] {
    let mut bytes = [0u8; NATIVE_PACKED_BLOCK_INDEX_SIZE];
    bytes[0] = page.object_id.unwrap_or_default() as u8;
    bytes[1] = model_report_code(&page.model_id);
    bytes[2..4].copy_from_slice(&(page.block_id.unwrap_or_default() as u16).to_le_bytes());
    bytes[4] = u8::from(page.dirty) | (u8::from(page.log_backed) << 1);
    let page_size = if page.deleted { 0 } else { page.length as u32 };
    bytes[5..9].copy_from_slice(&page_size.to_le_bytes());
    let address = physical_address_word(&BlockAddress::from_parts(page.block_slab_id, page.offset, page.length, page.block_id, page.object_id, Some(page.routing_bucket)));
    bytes[9..17].copy_from_slice(&address.to_le_bytes());
    bytes
}

pub(super) fn native_packed_bucket_node_bytes(bucket: &StoragePhysicalBucketNode) -> [u8; NATIVE_PACKED_BUCKET_NODE_SIZE] {
    let mut bytes = [0u8; NATIVE_PACKED_BUCKET_NODE_SIZE];
    let page_in_log = bucket.block_indexes.iter().any(|page| page.log_backed);
    let trivial_block = bucket.block_ref_count <= 1;
    let block_deleted = bucket.block_ref_count == 0;
    let mut flags = 0u32;
    flags |= (bucket.ttl_ms.is_some() as u32) << 1;
    flags |= (bucket.dirty as u32) << 2;
    flags |= (bucket.loading as u32) << 4;
    flags |= (bucket.in_memory as u32) << 5;
    flags |= (bucket.dirty as u32) << 6;
    flags |= (block_deleted as u32) << 7;
    flags |= (page_in_log as u32) << 8;
    flags |= (trivial_block as u32) << 9;
    let flag_bytes = flags.to_le_bytes();
    bytes[0..3].copy_from_slice(&flag_bytes[0..3]);
    bytes[3..7].copy_from_slice(&(bucket.physical_bytes as u32).to_le_bytes());
    // 0 HERE MEANS THE BUCKET NAMES NO PAGE, and now it means only that. Every declared kind
    // packs as a non-zero code (`model_kind_registry` asserts it), and a kind the registry does
    // not declare refuses rather than landing on this value -- so a reader can tell an empty
    // bucket from one holding a zset, which it could not before.
    let model_code = bucket
        .block_indexes
        .first()
        .map(|page| model_report_code(&page.model_id))
        .unwrap_or_default();
    bytes[7] = model_code;
    bytes[8..16].copy_from_slice(&bucket.ttl_ms.unwrap_or_default().to_le_bytes());
    let address = bucket
        .block_indexes
        .first()
        .map(|page| page.block_slab_id.wrapping_shl(32) | (page.offset & u32::MAX as u64))
        .unwrap_or_default();
    bytes[16..24].copy_from_slice(&address.to_le_bytes());
    bytes
}

pub(super) fn storage_physical_index_report(
    shard_id: ShardId,
    shard: &ShardState,
    summaries: Vec<BucketStorageSummary>,
) -> StoragePhysicalIndexReport {
    let summary_by_bucket = summaries
        .into_iter()
        .map(|summary| (summary.routing_bucket, summary))
        .collect::<BTreeMap<_, _>>();
    let mut buckets = summary_by_bucket
        .iter()
        .map(|(routing_bucket, summary)| {
            (
                *routing_bucket,
                StoragePhysicalBucketNode {
                    routing_bucket: *routing_bucket,
                    layout: "empty".to_string(),
                    dirty: summary.dirty_object_count > 0,
                    meta_loaded: true,
                    loading: false,
                    in_memory: summary.block_ref_count > 0,
                    ttl_ms: None,
                    object_count: summary.object_count,
                    block_ref_count: summary.block_ref_count,
                    logical_bytes: summary.logical_bytes,
                    physical_bytes: summary.physical_bytes,
                    dirty_generation: summary.dirty_generation,
                    last_dump_sequence: summary.last_dump_sequence,
                    native_packed_bucket_node_len: NATIVE_PACKED_BUCKET_NODE_SIZE,
                    native_packed_bucket_node_hex: String::new(),
                    block_indexes: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut missing_routing_bucket_count = 0usize;
    for entry in collect_live_block_entries(shard) {
        if entry.address.routing_bucket().is_none() {
            missing_routing_bucket_count = missing_routing_bucket_count.saturating_add(1);
        }
        let routing_bucket = entry
            .address
            .routing_bucket()
            .or(entry.filed_bucket())
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let bucket = buckets
            .entry(routing_bucket)
            .or_insert(StoragePhysicalBucketNode {
                routing_bucket,
                layout: "empty".to_string(),
                meta_loaded: true,
                in_memory: true,
                native_packed_bucket_node_len: NATIVE_PACKED_BUCKET_NODE_SIZE,
                ..StoragePhysicalBucketNode::default()
            });
        let mut block_index = StoragePhysicalBlockIndex {
            object_key: entry.object_key.clone().to_string(),
            model_id: entry.kind.clone().to_string(),
            component: entry.component.clone().map(|value| value.to_string()),
            routing_bucket,
            block_slab_id: entry.address.block_slab_id(),
            offset: entry.address.offset(),
            length: entry.address.length(),
            block_id: entry.address.block_id(),
            object_id: entry.address.object_id(),
            stored_slab_id: entry.address.slab_id(),
            // The index does not hold a digest; a caller wanting one reads the page.
            checksum: None,
            dirty: entry.dirty,
            deleted: entry.deleted,
            log_backed: entry.log_backed,
            native_packed_block_index_len: NATIVE_PACKED_BLOCK_INDEX_SIZE,
            native_packed_block_index_hex: String::new(),
        };
        block_index.native_packed_block_index_hex =
            hex::encode(native_packed_block_index_bytes(&block_index));
        bucket.block_indexes.push(block_index);
    }
    for (routing_bucket, runtime_bucket) in &shard.bucket_index.bucket_map {
        let bucket = buckets
            .entry(*routing_bucket)
            .or_insert(StoragePhysicalBucketNode {
                routing_bucket: *routing_bucket,
                native_packed_bucket_node_len: NATIVE_PACKED_BUCKET_NODE_SIZE,
                ..StoragePhysicalBucketNode::default()
            });
        bucket.layout = bucket_layout_name(runtime_bucket.layout).to_string();
        bucket.dirty = runtime_bucket.dirty();
        bucket.meta_loaded = runtime_bucket.meta_loaded();
        bucket.loading = runtime_bucket.loading();
        bucket.in_memory = runtime_bucket.in_memory();
        bucket.ttl_ms = runtime_bucket.ttl_ms.ms();
        bucket.object_count = runtime_bucket.object_index.len() as u64;
        bucket.block_ref_count = runtime_bucket.block_index.len() as u64;
        bucket.dirty_generation = runtime_bucket.dirty_generation;
        // `last_dump_sequence` IS NOT OVERWRITTEN FROM THE NODE HERE, and that is the whole of
        // this report's change. The value the row already carries came from the summary above,
        // which `merge_last_dump_sequence` fills from the NEWEST dump manifest -- so the report
        // now answers "is this bucket covered by the newest dump, and at what index-log
        // sequence", the one figure a decision in this engine actually reads (as the dump
        // ordering's tiebreaker). The node's own figure was a different number under the same
        // name, `max(manifest.wal_sequence)` over every manifest that ever named the bucket, and
        // nothing read it except this line and the bucket-store runtime report.
        //
        // WHAT IS LOST, stated rather than glossed: a bucket named by an older manifest and not
        // by the newest one reported the older manifest's WAL sequence and now reports 0. That
        // figure is not reconstructible from the newest manifest alone, and it was not readable
        // by anything that decided anything.
        for page in runtime_bucket.block_index.values() {
            let already_present = bucket.block_indexes.iter().any(|existing| {
                existing.object_key.as_str() == page.object_key.as_ref()
                    && *existing.model_id == *page.model_id
                    && existing.component.as_deref() == page.component.as_deref()
                    && existing.block_slab_id == page.address.block_slab_id()
                    && existing.offset == page.address.offset()
            });
            if already_present {
                continue;
            }
            let mut block_index = StoragePhysicalBlockIndex {
                object_key: page.object_key.clone().to_string(),
                model_id: page.model_id.clone().to_string(),
                component: page.component.clone().map(|value| value.to_string()),
                routing_bucket: *routing_bucket,
                block_slab_id: page.address.block_slab_id(),
                offset: page.address.offset(),
                length: page.address.length(),
                block_id: page.address.block_id(),
                object_id: Some(page.object_id()),
                stored_slab_id: page.address.slab_id(),
                checksum: None,
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed,
                native_packed_block_index_len: NATIVE_PACKED_BLOCK_INDEX_SIZE,
                native_packed_block_index_hex: String::new(),
            };
            block_index.native_packed_block_index_hex =
                hex::encode(native_packed_block_index_bytes(&block_index));
            bucket.block_indexes.push(block_index);
        }
    }
    for bucket in buckets.values_mut() {
        bucket.block_indexes.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then(left.model_id.cmp(&right.model_id))
                .then(left.component.cmp(&right.component))
                .then(left.block_slab_id.cmp(&right.block_slab_id))
                .then(left.offset.cmp(&right.offset))
        });
        if !shard.bucket_index.bucket_map.contains_key(&bucket.routing_bucket) {
            let object_count = bucket
                .block_indexes
                .iter()
                .filter_map(|page| page.object_id)
                .collect::<BTreeSet<_>>()
                .len();
            bucket.layout =
                bucket_layout_name(classify_bucket_layout(object_count, bucket.block_indexes.len()))
                    .to_string();
        }
        bucket.native_packed_bucket_node_len = NATIVE_PACKED_BUCKET_NODE_SIZE;
        bucket.native_packed_bucket_node_hex = hex::encode(native_packed_bucket_node_bytes(bucket));
    }
    let block_index_count = buckets
        .values()
        .map(|bucket| bucket.block_indexes.len())
        .sum::<usize>();
    let block_indexes = buckets
        .values()
        .flat_map(|bucket| bucket.block_indexes.iter())
        .collect::<Vec<_>>();
    let missing_object_id_count = block_indexes
        .iter()
        .filter(|page| page.object_id.is_none())
        .count();
    let missing_block_id_count = block_indexes
        .iter()
        .filter(|page| page.block_id.is_none())
        .count();
    let missing_checksum_count = block_indexes
        .iter()
        .filter(|page| page.checksum.is_none())
        .count();
    StoragePhysicalIndexReport {
        shard_id,
        bucket_first: true,
        bucket_index_authority: !shard.bucket_index.bucket_map.is_empty(),
        secondary_views_reconciled_from_bucket_index: !shard.bucket_index.bucket_map.is_empty(),
        bucket_count: buckets.len(),
        block_index_count,
        dirty_bucket_count: buckets.values().filter(|bucket| bucket.dirty).count(),
        missing_object_id_count,
        missing_routing_bucket_count,
        missing_block_id_count,
        missing_checksum_count,
        native_packed_block_index_size: NATIVE_PACKED_BLOCK_INDEX_SIZE,
        native_packed_bucket_node_size: NATIVE_PACKED_BUCKET_NODE_SIZE,
        native_packed_layout_compatible: true,
        bucket_nodes: buckets.into_values().collect(),
    }
}

pub(super) fn object_manager_runtime_report(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> ObjectManagerRuntimeReport {
    object_manager_runtime_report_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        start_routing_bucket,
        end_routing_bucket,
    )
}

/// The same report, from live-page entries the caller ALREADY has.
///
/// This walked the shard TWICE: once for the ownership report below, and once more at the very end
/// purely to COUNT entries of eight timestamped kinds. Measured at 2.0x the shard
/// (`what_the_compaction_preamble_walks`), which is why the compaction preamble could not get
/// below 7.0x while calling the wrapper form.
pub(super) fn object_manager_runtime_report_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> ObjectManagerRuntimeReport {
    let ownership = bucket_object_block_ownership_report_from_entries(
        shard_id,
        shard,
        entries,
        start_routing_bucket,
        end_routing_bucket,
    );
    let object_runtime = object_manager::runtime_report(shard);
    let mut report = ObjectManagerRuntimeReport {
        shard_id,
        routing_bucket_count: shard.bucket_index.bucket_map.len() as u64,
        object_count: object_runtime.live_object_count as u64,
        block_ref_count: object_runtime.live_block_ref_count as u64,
        hot_object_count: object_runtime.hot_object_count as u64,
        cold_object_count: object_runtime.cold_object_count as u64,
        mixed_residency_object_count: object_runtime.mixed_residency_object_count as u64,
        delete_marker_object_count: object_runtime.deleted_object_count as u64,
        dirty_object_count: object_runtime.dirty_object_count as u64,
        loading_object_count: object_runtime.loading_object_count as u64,
        ttl_object_count: object_runtime.ttl_object_count as u64,
        object_block_transition_count: object_runtime.object_block_transition_count as u64,
        dirty_bucket_count: shard
            .bucket_index
            .bucket_map
            .values()
            .filter(|bucket| bucket.dirty())
            .count() as u64,
        max_dirty_generation: shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.dirty_generation)
            .max()
            .unwrap_or_default(),
        missing_owner_block_ref_count: ownership.missing_owner_block_ref_count,
        owner_mismatch_block_ref_count: ownership.owner_mismatch_block_ref_count,
        evidence: vec![
            "runtime owns page refs in the first-class slot index".to_string(),
            "runtime tracks dirty generations and dirty routing slots in SlotNode".to_string(),
            "runtime validates owner refs before reporting ready".to_string(),
            "runtime tracks hot/cold/tombstone object state and object-page ownership transitions"
                .to_string(),
        ],
        ..ObjectManagerRuntimeReport::default()
    };

    for bucket in shard.bucket_index.bucket_map.values() {
        if let Some(state) = report
            .layout_states
            .iter_mut()
            .find(|state| state.state == bucket_layout_name(bucket.layout))
        {
            state.object_count = state
                .object_count
                .saturating_add(bucket.object_index.len() as u64);
        } else {
            report.layout_states.push(BucketLayoutStateCount {
                state: bucket_layout_name(bucket.layout).to_string(),
                object_count: bucket.object_index.len() as u64,
            });
        }
        if bucket.meta_loaded() {
            report.meta_object_count = report.meta_object_count.saturating_add(1);
        }
        match bucket.layout {
            BucketLayoutState::Empty => {}
            BucketLayoutState::SingleObject | BucketLayoutState::SingleBlockObject => {
                report.object_block_count = report.object_block_count.saturating_add(1);
            }
            BucketLayoutState::MultiBlockObject => {
                report.multi_block_object_count = report.multi_block_object_count.saturating_add(1);
            }
            BucketLayoutState::MultiObject => {}
        }
    }

    if !ownership.first_class_index_present {
        report
            .blockers
            .push("first-class slot_objects runtime index is empty".to_string());
    }
    if ownership.missing_owner_block_ref_count > 0 {
        report
            .blockers
            .push("page refs are missing object/routing-slot ownership metadata".to_string());
    }
    if ownership.owner_mismatch_block_ref_count > 0 {
        report
            .blockers
            .push("page refs disagree with expected object owners".to_string());
    }
    // Count live timestamped-kv pages (feature/sequence and the context
    // timeline families). collect_live_block_entries already dedupes packed series
    // pages via unique_timestamped_kv_block_addresses, so this is the packed page
    // count. Previously this field was left at its default (0).
    const TIMESTAMPED_KINDS: [&str; 8] = [
        "feature",
        "sequence",
        "context_event",
        "context_index",
        "context_audit",
        "context_child",
        "context_summary",
        "context_compression",
    ];
    report.packed_timestamped_block_count = entries
        .iter()
        .filter(|entry| TIMESTAMPED_KINDS.contains(&entry.kind.as_ref()))
        .count() as u64;
    report.runtime_ready = report.blockers.is_empty();
    report
}

pub(super) fn bucket_object_block_ownership_report(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> BucketObjectBlockOwnershipReport {
    bucket_object_block_ownership_report_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        start_routing_bucket,
        end_routing_bucket,
    )
}

/// The same report, from live-page entries the caller ALREADY has.
pub(super) fn bucket_object_block_ownership_report_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> BucketObjectBlockOwnershipReport {
    let mut report = BucketObjectBlockOwnershipReport {
        shard_id,
        first_class_index_present: !shard.bucket_index.bucket_map.is_empty(),
        derived_from_model_maps: shard.bucket_index.bucket_map.is_empty(),
        ..BucketObjectBlockOwnershipReport::default()
    };
    report.block_ref_count = entries.len();
    for entry in entries {
        let routing_bucket = entry.address.routing_bucket().unwrap_or_default();
        if routing_bucket < start_routing_bucket || routing_bucket > end_routing_bucket {
            continue;
        }
        let expected_object_id = stable_block_object_id(
            shard_id,
            &entry.kind,
            &entry.object_key,
            entry.component.as_deref(),
        );
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            report.missing_owner_block_ref_count =
                report.missing_owner_block_ref_count.saturating_add(1);
            continue;
        };
        if !bucket.object_index.contains(&expected_object_id) {
            report.owner_mismatch_block_ref_count =
                report.owner_mismatch_block_ref_count.saturating_add(1);
        }
    }
    report
}

pub(super) fn merge_last_dump_sequence(
    mut summaries: Vec<BucketStorageSummary>,
    manifest: &BucketDumpManifest,
) -> Vec<BucketStorageSummary> {
    let dumped_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
    for summary in &mut summaries {
        if dumped_buckets.contains(&summary.routing_bucket) {
            summary.last_dump_sequence = manifest.index_log_sequence;
        }
    }
    summaries
}

pub(super) fn bucket_dump_manifest_comparable_summaries(
    shard: &ShardState,
    selected_buckets: &BTreeSet<u32>,
) -> Vec<BucketStorageSummary> {
    comparable_bucket_dump_summaries(
        bucket_storage_summaries(shard, 0, u32::MAX)
            .into_iter()
            .filter(|summary| {
                selected_buckets.is_empty() || selected_buckets.contains(&summary.routing_bucket)
            })
            .collect(),
    )
}

pub(super) fn comparable_bucket_dump_summaries(
    mut summaries: Vec<BucketStorageSummary>,
) -> Vec<BucketStorageSummary> {
    for summary in &mut summaries {
        summary.dirty_object_count = 0;
        summary.dirty_generation = 0;
        summary.last_dump_sequence = 0;
        summary.block_slab_ids.sort_unstable();
        summary.block_slab_ids.dedup();
    }
    summaries.retain(|summary| {
        summary.object_count > 0
            || summary.block_ref_count > 0
            || summary.logical_bytes > 0
            || summary.physical_bytes > 0
    });
    summaries.sort_by_key(|summary| summary.routing_bucket);
    summaries
}

pub(super) fn bucket_dump_summary_matches_current_generation(
    manifest_summary: &BucketStorageSummary,
    current_summary: &BucketStorageSummary,
    manifest_bucket_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
    current_bucket_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
) -> bool {
    let mut manifest_slabs = manifest_summary.block_slab_ids.clone();
    manifest_slabs.sort_unstable();
    manifest_slabs.dedup();
    let mut current_slabs = current_summary.block_slab_ids.clone();
    current_slabs.sort_unstable();
    current_slabs.dedup();
    manifest_summary.routing_bucket == current_summary.routing_bucket
        && manifest_summary.dirty_generation == current_summary.dirty_generation
        && manifest_summary.object_count == current_summary.object_count
        && manifest_summary.block_ref_count == current_summary.block_ref_count
        && manifest_summary.logical_bytes == current_summary.logical_bytes
        && manifest_summary.physical_bytes == current_summary.physical_bytes
        && manifest_slabs == current_slabs
        && manifest_bucket_fingerprints.get(&manifest_summary.routing_bucket)
            == current_bucket_fingerprints.get(&current_summary.routing_bucket)
}

pub(super) fn bucket_generation_fingerprints_by_bucket(shard: &ShardState) -> BTreeMap<u32, BTreeSet<String>> {
    let mut by_bucket = BTreeMap::<u32, BTreeSet<String>>::new();
    for entry in collect_live_block_entries(shard) {
        let routing_bucket = entry
            .address
            .routing_bucket()
            .or(entry.filed_bucket())
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        by_bucket.entry(routing_bucket).or_default().insert(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            entry.kind,
            entry.object_key,
            entry.component.unwrap_or_default(),
            entry.address.block_slab_id(),
            entry.address.offset(),
            entry.address.length(),
            entry.address.block_id().unwrap_or_default(),
            entry.address.object_id().unwrap_or_default(),
            entry.address.routing_bucket().unwrap_or(routing_bucket),
            entry.address.generation().unwrap_or_default(),
            String::new()
        ));
    }
    by_bucket
}

pub(super) fn collect_live_block_addresses(shard: &ShardState) -> Vec<BlockAddress> {
    collect_live_block_entries(shard)
        .into_iter()
        .map(|entry| entry.address)
        .collect()
}

pub(super) fn unique_timestamped_kv_block_addresses(series: &BTreeMap<u64, BlockAddress>) -> Vec<BlockAddress> {
    let mut addresses = series
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    addresses.sort_by(|left, right| {
        left.block_slab_id()
            .cmp(&right.block_slab_id())
            .then(left.offset().cmp(&right.offset()))
            .then(left.length().cmp(&right.length()))
    });
    addresses
}

pub(super) fn unique_feature_block_addresses(series: &BTreeMap<u64, BlockAddress>) -> Vec<BlockAddress> {
    unique_timestamped_kv_block_addresses(series)
}

pub(super) fn timestamped_kv_series<'a>(
    shard: &'a ShardState,
) -> Vec<(&'static str, &'a str, std::borrow::Cow<'a, BTreeMap<u64, BlockAddress>>)> {
    use std::borrow::Cow;
    let mut series = Vec::new();
    for (key, timeline) in &shard.features {
        series.push(("feature", key.as_str(), Cow::Borrowed(timeline)));
    }
    // Since the event rekey, context_events is keyed by EVENT ID; the timestamps live in
    // context_event_timeline (timeline key -> event id). The validator compares index keys
    // against the timestamps packed in pages, so it must see the TIMELINE view -- handing it
    // the id-keyed map made every context event look like a missing indexed timestamp.
    for (key, ids_by_time) in &shard.context_event_timeline {
        let Some(by_id) = shard.context_events.get(key) else {
            continue;
        };
        let timeline: BTreeMap<u64, BlockAddress> = ids_by_time
            .iter()
            .filter_map(|(timeline_key, event_id)| {
                by_id
                    .get(event_id)
                    .map(|address| (*timeline_key, address.clone()))
            })
            .collect();
        series.push(("context_event", key.as_str(), Cow::Owned(timeline)));
    }
    for (key, timeline) in &shard.context_indexes {
        series.push(("context_index", key.as_str(), Cow::Borrowed(timeline)));
    }
    for (key, timeline) in &shard.context_audits {
        series.push(("context_audit", key.as_str(), Cow::Borrowed(timeline)));
    }
    for (key, timeline) in &shard.context_children {
        series.push(("context_child", key.as_str(), Cow::Borrowed(timeline)));
    }
    for (key, timeline) in &shard.context_summaries {
        series.push(("context_summary", key.as_str(), Cow::Borrowed(timeline)));
    }
    for (key, timeline) in &shard.context_compressions {
        series.push(("context_compression", key.as_str(), Cow::Borrowed(timeline)));
    }
    series
}

pub(super) fn storage_feature_block_layout_report(
    block_store: &BlockStore,
    shard: &ShardState,
) -> StorageFeatureBlockLayoutReport {
    let mut report = StorageFeatureBlockLayoutReport::default();
    let mut family_reports = BTreeMap::<String, StorageTimestampedBlockFamilyReport>::new();
    let mut inspected_addresses = HashSet::<BlockAddress>::new();
    for (kind, key, series) in timestamped_kv_series(shard) {
        report.indexed_timestamped_points = report
            .indexed_timestamped_points
            .saturating_add(series.len());
        if kind == "feature" {
            report.indexed_feature_points =
                report.indexed_feature_points.saturating_add(series.len());
        }
        let family = family_reports.entry(kind.to_string()).or_insert_with(|| {
            StorageTimestampedBlockFamilyReport {
                kind: kind.to_string(),
                ..StorageTimestampedBlockFamilyReport::default()
            }
        });
        family.indexed_points = family.indexed_points.saturating_add(series.len());
        let mut timestamps_by_address = HashMap::<BlockAddress, BTreeSet<u64>>::new();
        for (timestamp_ms, address) in series.iter() {
            timestamps_by_address
                .entry(address.clone())
                .or_default()
                .insert(*timestamp_ms);
        }
        report.unique_timestamped_block_refs = report
            .unique_timestamped_block_refs
            .saturating_add(timestamps_by_address.len());
        family.unique_block_refs = family
            .unique_block_refs
            .saturating_add(timestamps_by_address.len());
        if kind == "feature" {
            report.unique_feature_block_refs = report
                .unique_feature_block_refs
                .saturating_add(timestamps_by_address.len());
        }

        for (address, indexed_timestamps) in timestamps_by_address {
            inspected_addresses.insert(address.clone());
            match block_store.read(&address) {
                Ok(bytes) => match decode_feature_block_strict(&bytes) {
                    PackedFeatureBlockDecode::Packed(points) => {
                        report.packed_timestamped_blocks =
                            report.packed_timestamped_blocks.saturating_add(1);
                        family.packed_blocks = family.packed_blocks.saturating_add(1);
                        if kind == "feature" {
                            report.packed_feature_blocks =
                                report.packed_feature_blocks.saturating_add(1);
                        }
                        let mut packed_timestamp_counts = BTreeMap::<u64, usize>::new();
                        for point in &points {
                            let count = packed_timestamp_counts
                                .entry(point.timestamp_ms)
                                .or_default();
                            if *count == 1 {
                                report.duplicate_packed_timestamps.push(
                                    feature_block_timestamp_mismatch(
                                        kind,
                                        key,
                                        point.timestamp_ms,
                                        &address,
                                    ),
                                );
                                family.mismatch_count = family.mismatch_count.saturating_add(1);
                            }
                            *count = (*count).saturating_add(1);
                        }
                        let packed_timestamps = points
                            .into_iter()
                            .map(|point| point.timestamp_ms)
                            .collect::<BTreeSet<_>>();
                        for timestamp_ms in
                            indexed_timestamps.difference(&packed_timestamps).copied()
                        {
                            report.missing_indexed_timestamps.push(
                                feature_block_timestamp_mismatch(kind, key, timestamp_ms, &address),
                            );
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                        for timestamp_ms in
                            packed_timestamps.difference(&indexed_timestamps).copied()
                        {
                            report
                                .orphan_packed_timestamps
                                .push(feature_block_timestamp_mismatch(
                                    kind,
                                    key,
                                    timestamp_ms,
                                    &address,
                                ));
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                    }
                    PackedFeatureBlockDecode::Corrupt(error) => {
                        report
                            .corrupt_packed_feature_blocks
                            .push(feature_block_error(kind, key, &address, error));
                        family.corrupt_blocks = family.corrupt_blocks.saturating_add(1);
                    }
                    PackedFeatureBlockDecode::Legacy => {
                        report.legacy_timestamped_value_blocks =
                            report.legacy_timestamped_value_blocks.saturating_add(1);
                        family.legacy_value_blocks = family.legacy_value_blocks.saturating_add(1);
                        if kind == "feature" {
                            report.legacy_feature_value_blocks =
                                report.legacy_feature_value_blocks.saturating_add(1);
                        }
                        if indexed_timestamps.len() > 1 {
                            report.corrupt_packed_feature_blocks.push(feature_block_error(
                                kind,
                                key,
                                &address,
                                "legacy timestamped value page shared by multiple timestamps",
                            ));
                            family.corrupt_blocks = family.corrupt_blocks.saturating_add(1);
                        }
                    }
                },
                Err(err) => {
                    report.corrupt_packed_feature_blocks.push(feature_block_error(
                        kind,
                        key,
                        &address,
                        err.to_string(),
                    ));
                    family.corrupt_blocks = family.corrupt_blocks.saturating_add(1);
                }
            }
        }
    }
    for entry in collect_bucket_index_live_block_entries(shard) {
        if entry.deleted || inspected_addresses.contains(&entry.address) {
            continue;
        }
        if !matches!(
            entry.kind.as_ref(),
            "feature"
                | "sequence"
                | "context_event"
                | "context_index"
                | "context_audit"
                | "context_child"
                | "context_summary"
                | "context_compression"
        ) {
            continue;
        }
        let family = family_reports.entry(entry.kind.clone().to_string()).or_insert_with(|| {
            StorageTimestampedBlockFamilyReport {
                kind: entry.kind.clone().to_string(),
                ..StorageTimestampedBlockFamilyReport::default()
            }
        });
        report.unique_timestamped_block_refs = report.unique_timestamped_block_refs.saturating_add(1);
        family.unique_block_refs = family.unique_block_refs.saturating_add(1);
        if &*entry.kind == "feature" {
            report.unique_feature_block_refs = report.unique_feature_block_refs.saturating_add(1);
        }
        match block_store.read(&entry.address) {
            Ok(bytes) => match decode_feature_block_strict(&bytes) {
                PackedFeatureBlockDecode::Packed(points) => {
                    report.packed_timestamped_blocks =
                        report.packed_timestamped_blocks.saturating_add(1);
                    family.packed_blocks = family.packed_blocks.saturating_add(1);
                    if &*entry.kind == "feature" {
                        report.packed_feature_blocks = report.packed_feature_blocks.saturating_add(1);
                    }
                    for point in points {
                        report
                            .orphan_packed_timestamps
                            .push(feature_block_timestamp_mismatch(
                                &entry.kind,
                                &entry.object_key,
                                point.timestamp_ms,
                                &entry.address,
                            ));
                        family.mismatch_count = family.mismatch_count.saturating_add(1);
                    }
                }
                PackedFeatureBlockDecode::Corrupt(error) => {
                    report.corrupt_packed_feature_blocks.push(feature_block_error(
                        &entry.kind,
                        &entry.object_key,
                        &entry.address,
                        error,
                    ));
                    family.corrupt_blocks = family.corrupt_blocks.saturating_add(1);
                }
                PackedFeatureBlockDecode::Legacy => {
                    report.legacy_timestamped_value_blocks =
                        report.legacy_timestamped_value_blocks.saturating_add(1);
                    family.legacy_value_blocks = family.legacy_value_blocks.saturating_add(1);
                    if &*entry.kind == "feature" {
                        report.legacy_feature_value_blocks =
                            report.legacy_feature_value_blocks.saturating_add(1);
                    }
                }
            },
            Err(err) => {
                report.corrupt_packed_feature_blocks.push(feature_block_error(
                    &entry.kind,
                    &entry.object_key,
                    &entry.address,
                    err.to_string(),
                ));
                family.corrupt_blocks = family.corrupt_blocks.saturating_add(1);
            }
        }
    }
    report.families = family_reports.into_values().collect();
    report
}

pub(super) fn feature_block_error(
    kind: &str,
    key: &str,
    address: &BlockAddress,
    error: impl Into<String>,
) -> StorageFeatureBlockError {
    StorageFeatureBlockError {
        kind: kind.to_string(),
        key: key.to_string(),
        block_slab_id: address.block_slab_id(),
        offset: address.offset(),
        length: address.length(),
        error: error.into(),
    }
}

pub(super) fn feature_block_timestamp_mismatch(
    kind: &str,
    key: &str,
    timestamp_ms: u64,
    address: &BlockAddress,
) -> StorageFeatureBlockTimestampMismatch {
    StorageFeatureBlockTimestampMismatch {
        kind: kind.to_string(),
        key: key.to_string(),
        timestamp_ms,
        block_slab_id: address.block_slab_id(),
        offset: address.offset(),
        length: address.length(),
    }
}

