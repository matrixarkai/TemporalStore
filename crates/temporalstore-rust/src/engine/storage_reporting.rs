//! Storage physical-index / object-manager / feature-page layout report helpers, split from engine.rs.
use super::*;

pub(super) fn storage_object_lifecycle_report(
    shard_id: ShardId,
    shard: &ShardState,
) -> StorageObjectLifecycleReport {
    storage_object_lifecycle_report_for_slots(shard_id, shard, &BTreeSet::new(), |_| 0)
}

pub(super) fn storage_object_lifecycle_report_for_slots(
    shard_id: ShardId,
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
    routing_slot_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    let entries = collect_live_page_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| routing_slot_for_key(&entry.object_key));
            selected_slots.is_empty() || selected_slots.contains(&routing_slot)
        })
        .collect::<Vec<_>>();
    let mut expected_object_ids = BTreeSet::new();
    let mut actual_object_owners = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut missing_owner_page_refs = 0u64;
    let mut owner_mismatch_page_refs = 0u64;

    for entry in &entries {
        let expected_object_id = expected_live_page_object_id(shard_id, entry);
        expected_object_ids.insert(expected_object_id);
        if entry.address.object_id.is_none() || entry.address.routing_slot.is_none() {
            missing_owner_page_refs = missing_owner_page_refs.saturating_add(1);
        }
        match entry.address.object_id {
            Some(actual_object_id) => {
                actual_object_owners
                    .entry(actual_object_id)
                    .or_default()
                    .insert(expected_object_id);
                if actual_object_id != expected_object_id {
                    owner_mismatch_page_refs = owner_mismatch_page_refs.saturating_add(1);
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
    let tombstoned_object_keys = shard
        .dirty_objects
        .iter()
        .filter(|key| {
            let routing_slot = routing_slot_for_key(key);
            (selected_slots.is_empty() || selected_slots.contains(&routing_slot))
                && !record_exists(shard, key)
        })
        .cloned()
        .collect::<Vec<_>>();

    StorageObjectLifecycleReport {
        live_object_ids: expected_object_ids.len() as u64,
        live_page_refs: entries.len() as u64,
        stale_object_ids: 0,
        tombstoned_object_ids: tombstoned_object_keys.len() as u64,
        reused_object_id_conflicts: reused_object_ids.len() as u64,
        missing_owner_page_refs,
        owner_mismatch_page_refs,
        reused_object_ids,
        tombstoned_object_keys,
    }
}

pub(super) fn slot_dump_entries_by_key(
    shard_id: ShardId,
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
    routing_slot_for_key: impl Fn(&str) -> u32,
) -> BTreeMap<String, PageAddress> {
    collect_live_page_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| routing_slot_for_key(&entry.object_key));
            selected_slots.is_empty() || selected_slots.contains(&routing_slot)
        })
        .map(|entry| {
            let component = entry.component.unwrap_or_default();
            let page_id = entry.address.page_id.unwrap_or_else(|| {
                stable_page_object_id(
                    shard_id,
                    &entry.kind,
                    &entry.object_key,
                    (!component.is_empty()).then_some(component.as_str()),
                )
            });
            (
                format!(
                    "{}:{}:{}:{}",
                    entry.kind, entry.object_key, component, page_id
                ),
                entry.address,
            )
        })
        .collect()
}

pub(super) fn slot_storage_summaries(
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> Vec<SlotStorageSummary> {
    let mut slots = BTreeMap::<u32, SlotStorageSummary>::new();
    let mut page_segments_by_slot = BTreeMap::<u32, BTreeSet<u64>>::new();
    for entry in collect_live_page_entries(shard) {
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.page_ref_count = summary.page_ref_count.saturating_add(1);
        summary.physical_bytes = summary.physical_bytes.saturating_add(entry.address.length);
        summary.logical_bytes = summary.logical_bytes.saturating_add(entry.address.length);
        // Record which page segment backs each slot so slot-dump manifests carry the
        // live segment set (used by manifest validation and the dump/copy path).
        // Without this the map stayed empty and every summary reported no segments.
        page_segments_by_slot
            .entry(routing_slot)
            .or_default()
            .insert(entry.address.page_segment_id);
        if let Some(zone_id) = entry.address.band_id {
            summary.last_compacted_zone = Some(
                summary
                    .last_compacted_zone
                    .map_or(zone_id, |current| current.max(zone_id)),
            );
        }
    }
    for (routing_slot, slot) in &shard.slot_index.slot_map {
        if !slot.dirty {
            continue;
        }
        let summary = slots.entry(*routing_slot).or_insert(SlotStorageSummary {
            routing_slot: *routing_slot,
            ..SlotStorageSummary::default()
        });
        // object_index.len() is the slot's total object count, not its dirty count;
        // assigning it to dirty_object_count double-counted (the dirty_objects loop
        // below is the authoritative per-key dirty tally).
        summary.object_count = slot.object_index.len() as u64;
        summary.dirty_generation = slot.dirty_generation;
    }
    for key in &shard.dirty_objects {
        let routing_slot = page_routing_slot(key, start_routing_slot, end_routing_slot);
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.dirty_object_count = summary.dirty_object_count.saturating_add(1);
        summary.dirty_generation = summary.dirty_generation.saturating_add(1);
    }
    for (routing_slot, summary) in &mut slots {
        summary.page_segment_ids = page_segments_by_slot
            .get(routing_slot)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
    }
    slots.into_values().collect()
}

const CPP_PACKED_PAGE_INDEX_SIZE: usize = 17;
const CPP_PACKED_SLOT_NODE_SIZE: usize = 24;

pub(super) fn storage_model_code(kind: &str) -> u8 {
    match kind {
        "string" => 1,
        "hash" => 2,
        "set" => 3,
        "feature" => 4,
        "sequence" => 5,
        "ips" => 6,
        "risk" => 7,
        "context_node" => 8,
        "context_event" => 9,
        "context_index" => 10,
        "context_audit" => 11,
        "context_entity" => 13,
        "context_child" => 14,
        "context_embedding" => 15,
        "context_summary" => 16,
        "context_compression" => 17,
        _ => 0,
    }
}

pub(super) fn physical_address_word(address: &PageAddress) -> u64 {
    address.page_segment_id.wrapping_shl(32) | (address.offset & u32::MAX as u64)
}

pub(super) fn cpp_packed_page_index_bytes(
    page: &StoragePhysicalPageIndex,
) -> [u8; CPP_PACKED_PAGE_INDEX_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_PAGE_INDEX_SIZE];
    bytes[0] = page.object_id.unwrap_or_default() as u8;
    bytes[1] = storage_model_code(&page.model_id);
    bytes[2..4].copy_from_slice(&(page.page_id.unwrap_or_default() as u16).to_le_bytes());
    bytes[4] = u8::from(page.dirty) | (u8::from(page.log_backed) << 1);
    let page_size = if page.deleted { 0 } else { page.length as u32 };
    bytes[5..9].copy_from_slice(&page_size.to_le_bytes());
    let address = physical_address_word(&PageAddress {
        page_segment_id: page.page_segment_id,
        offset: page.offset,
        length: page.length,
        page_id: page.page_id,
        object_id: page.object_id,
        routing_slot: Some(page.routing_slot),
        generation: page.page_id.or(page.object_id),
        band_id: page.zone_id,
        sha256: page.checksum.clone(),
    });
    bytes[9..17].copy_from_slice(&address.to_le_bytes());
    bytes
}

pub(super) fn cpp_packed_slot_node_bytes(slot: &StoragePhysicalSlotNode) -> [u8; CPP_PACKED_SLOT_NODE_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_SLOT_NODE_SIZE];
    let page_in_log = slot.page_indexes.iter().any(|page| page.log_backed);
    let trivial_page = slot.page_ref_count <= 1;
    let page_deleted = slot.page_ref_count == 0;
    let mut flags = 0u32;
    flags |= (slot.ttl_ms.is_some() as u32) << 1;
    flags |= (slot.dirty as u32) << 2;
    flags |= (slot.loading as u32) << 4;
    flags |= (slot.in_memory as u32) << 5;
    flags |= (slot.dirty as u32) << 6;
    flags |= (page_deleted as u32) << 7;
    flags |= (page_in_log as u32) << 8;
    flags |= (trivial_page as u32) << 9;
    let flag_bytes = flags.to_le_bytes();
    bytes[0..3].copy_from_slice(&flag_bytes[0..3]);
    bytes[3..7].copy_from_slice(&(slot.physical_bytes as u32).to_le_bytes());
    let model_code = slot
        .page_indexes
        .first()
        .map(|page| storage_model_code(&page.model_id))
        .unwrap_or_default();
    bytes[7] = model_code;
    bytes[8..16].copy_from_slice(&slot.ttl_ms.unwrap_or_default().to_le_bytes());
    let address = slot
        .page_indexes
        .first()
        .map(|page| page.page_segment_id.wrapping_shl(32) | (page.offset & u32::MAX as u64))
        .unwrap_or_default();
    bytes[16..24].copy_from_slice(&address.to_le_bytes());
    bytes
}

pub(super) fn storage_physical_index_report(
    shard_id: ShardId,
    shard: &ShardState,
    summaries: Vec<SlotStorageSummary>,
) -> StoragePhysicalIndexReport {
    let summary_by_slot = summaries
        .into_iter()
        .map(|summary| (summary.routing_slot, summary))
        .collect::<BTreeMap<_, _>>();
    let mut slots = summary_by_slot
        .iter()
        .map(|(routing_slot, summary)| {
            (
                *routing_slot,
                StoragePhysicalSlotNode {
                    routing_slot: *routing_slot,
                    layout: "empty".to_string(),
                    dirty: summary.dirty_object_count > 0,
                    meta_loaded: true,
                    loading: false,
                    in_memory: summary.page_ref_count > 0,
                    ttl_ms: None,
                    object_count: summary.object_count,
                    page_ref_count: summary.page_ref_count,
                    logical_bytes: summary.logical_bytes,
                    physical_bytes: summary.physical_bytes,
                    dirty_generation: summary.dirty_generation,
                    last_dump_sequence: summary.last_dump_sequence,
                    cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                    cpp_packed_slot_node_hex: String::new(),
                    page_indexes: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut missing_routing_slot_count = 0usize;
    for entry in collect_live_page_entries(shard) {
        if entry.address.routing_slot.is_none() {
            missing_routing_slot_count = missing_routing_slot_count.saturating_add(1);
        }
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let slot = slots
            .entry(routing_slot)
            .or_insert(StoragePhysicalSlotNode {
                routing_slot,
                layout: "empty".to_string(),
                meta_loaded: true,
                in_memory: true,
                cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                ..StoragePhysicalSlotNode::default()
            });
        let mut page_index = StoragePhysicalPageIndex {
            object_key: entry.object_key.clone(),
            model_id: entry.kind.clone(),
            component: entry.component.clone(),
            routing_slot,
            page_segment_id: entry.address.page_segment_id,
            offset: entry.address.offset,
            length: entry.address.length,
            page_id: entry.address.page_id,
            object_id: entry.address.object_id,
            zone_id: entry.address.band_id,
            checksum: entry.address.sha256.clone(),
            dirty: entry.dirty,
            deleted: entry.deleted,
            log_backed: entry.log_backed,
            cpp_packed_page_index_len: CPP_PACKED_PAGE_INDEX_SIZE,
            cpp_packed_page_index_hex: String::new(),
        };
        page_index.cpp_packed_page_index_hex =
            hex::encode(cpp_packed_page_index_bytes(&page_index));
        slot.page_indexes.push(page_index);
    }
    for (routing_slot, runtime_slot) in &shard.slot_index.slot_map {
        let slot = slots
            .entry(*routing_slot)
            .or_insert(StoragePhysicalSlotNode {
                routing_slot: *routing_slot,
                cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                ..StoragePhysicalSlotNode::default()
            });
        slot.layout = slot_layout_name(runtime_slot.layout).to_string();
        slot.dirty = runtime_slot.dirty;
        slot.meta_loaded = runtime_slot.meta_loaded;
        slot.loading = runtime_slot.loading;
        slot.in_memory = runtime_slot.in_memory;
        slot.ttl_ms = runtime_slot.ttl_ms;
        slot.object_count = runtime_slot.object_index.len() as u64;
        slot.page_ref_count = runtime_slot.page_index.len() as u64;
        slot.dirty_generation = runtime_slot.dirty_generation;
        slot.last_dump_sequence = runtime_slot.last_dump_sequence;
        for page in runtime_slot.page_index.values() {
            let already_present = slot.page_indexes.iter().any(|existing| {
                existing.object_key == page.object_key
                    && existing.model_id == page.model_id
                    && existing.component == page.component
                    && existing.page_segment_id == page.address.page_segment_id
                    && existing.offset == page.address.offset
            });
            if already_present {
                continue;
            }
            let mut page_index = StoragePhysicalPageIndex {
                object_key: page.object_key.clone(),
                model_id: page.model_id.clone(),
                component: page.component.clone(),
                routing_slot: *routing_slot,
                page_segment_id: page.address.page_segment_id,
                offset: page.address.offset,
                length: page.address.length,
                page_id: page.address.page_id,
                object_id: Some(page.object_id),
                zone_id: page.address.band_id,
                checksum: page.address.sha256.clone(),
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed,
                cpp_packed_page_index_len: CPP_PACKED_PAGE_INDEX_SIZE,
                cpp_packed_page_index_hex: String::new(),
            };
            page_index.cpp_packed_page_index_hex =
                hex::encode(cpp_packed_page_index_bytes(&page_index));
            slot.page_indexes.push(page_index);
        }
    }
    for slot in slots.values_mut() {
        slot.page_indexes.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then(left.model_id.cmp(&right.model_id))
                .then(left.component.cmp(&right.component))
                .then(left.page_segment_id.cmp(&right.page_segment_id))
                .then(left.offset.cmp(&right.offset))
        });
        if !shard.slot_index.slot_map.contains_key(&slot.routing_slot) {
            let object_count = slot
                .page_indexes
                .iter()
                .filter_map(|page| page.object_id)
                .collect::<BTreeSet<_>>()
                .len();
            slot.layout =
                slot_layout_name(classify_slot_layout(object_count, slot.page_indexes.len()))
                    .to_string();
        }
        slot.cpp_packed_slot_node_len = CPP_PACKED_SLOT_NODE_SIZE;
        slot.cpp_packed_slot_node_hex = hex::encode(cpp_packed_slot_node_bytes(slot));
    }
    let page_index_count = slots
        .values()
        .map(|slot| slot.page_indexes.len())
        .sum::<usize>();
    let page_indexes = slots
        .values()
        .flat_map(|slot| slot.page_indexes.iter())
        .collect::<Vec<_>>();
    let missing_object_id_count = page_indexes
        .iter()
        .filter(|page| page.object_id.is_none())
        .count();
    let missing_page_id_count = page_indexes
        .iter()
        .filter(|page| page.page_id.is_none())
        .count();
    let missing_checksum_count = page_indexes
        .iter()
        .filter(|page| page.checksum.is_none())
        .count();
    StoragePhysicalIndexReport {
        shard_id,
        slot_first: true,
        slot_index_authority: !shard.slot_index.slot_map.is_empty(),
        secondary_views_reconciled_from_slot_index: !shard.slot_index.slot_map.is_empty(),
        slot_count: slots.len(),
        page_index_count,
        dirty_slot_count: slots.values().filter(|slot| slot.dirty).count(),
        missing_object_id_count,
        missing_routing_slot_count,
        missing_page_id_count,
        missing_checksum_count,
        cpp_packed_page_index_size: CPP_PACKED_PAGE_INDEX_SIZE,
        cpp_packed_slot_node_size: CPP_PACKED_SLOT_NODE_SIZE,
        cpp_packed_layout_compatible: true,
        slot_nodes: slots.into_values().collect(),
    }
}

pub(super) fn object_manager_runtime_report(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> ObjectManagerRuntimeReport {
    let ownership =
        slot_object_page_ownership_report(shard_id, shard, start_routing_slot, end_routing_slot);
    let object_runtime = object_manager::runtime_report(shard);
    let mut report = ObjectManagerRuntimeReport {
        shard_id,
        routing_slot_count: shard.slot_index.slot_map.len() as u64,
        object_count: object_runtime.live_object_count as u64,
        page_ref_count: object_runtime.live_page_ref_count as u64,
        hot_object_count: object_runtime.hot_object_count as u64,
        cold_object_count: object_runtime.cold_object_count as u64,
        mixed_residency_object_count: object_runtime.mixed_residency_object_count as u64,
        tombstone_object_count: object_runtime.deleted_object_count as u64,
        dirty_object_count: object_runtime.dirty_object_count as u64,
        loading_object_count: object_runtime.loading_object_count as u64,
        ttl_object_count: object_runtime.ttl_object_count as u64,
        object_page_transition_count: object_runtime.object_page_transition_count as u64,
        dirty_slot_count: shard
            .slot_index
            .slot_map
            .values()
            .filter(|slot| slot.dirty)
            .count() as u64,
        max_dirty_generation: shard
            .slot_index
            .slot_map
            .values()
            .map(|slot| slot.dirty_generation)
            .max()
            .unwrap_or_default(),
        missing_owner_page_ref_count: ownership.missing_owner_page_ref_count,
        owner_mismatch_page_ref_count: ownership.owner_mismatch_page_ref_count,
        evidence: vec![
            "runtime owns page refs in the first-class slot index".to_string(),
            "runtime tracks dirty generations and dirty routing slots in SlotNode".to_string(),
            "runtime validates owner refs before reporting ready".to_string(),
            "runtime tracks hot/cold/tombstone object state and object-page ownership transitions"
                .to_string(),
        ],
        ..ObjectManagerRuntimeReport::default()
    };

    for slot in shard.slot_index.slot_map.values() {
        if let Some(state) = report
            .layout_states
            .iter_mut()
            .find(|state| state.state == slot_layout_name(slot.layout))
        {
            state.object_count = state
                .object_count
                .saturating_add(slot.object_index.len() as u64);
        } else {
            report.layout_states.push(SlotLayoutStateCount {
                state: slot_layout_name(slot.layout).to_string(),
                object_count: slot.object_index.len() as u64,
            });
        }
        if slot.meta_loaded {
            report.meta_object_count = report.meta_object_count.saturating_add(1);
        }
        match slot.layout {
            SlotLayoutState::Empty => {}
            SlotLayoutState::SingleObject | SlotLayoutState::SinglePageObject => {
                report.object_page_count = report.object_page_count.saturating_add(1);
            }
            SlotLayoutState::MultiPageObject => {
                report.multi_page_object_count = report.multi_page_object_count.saturating_add(1);
            }
            SlotLayoutState::MultiObject => {}
        }
    }

    if !ownership.first_class_index_present {
        report
            .blockers
            .push("first-class slot_objects runtime index is empty".to_string());
    }
    if ownership.missing_owner_page_ref_count > 0 {
        report
            .blockers
            .push("page refs are missing object/routing-slot ownership metadata".to_string());
    }
    if ownership.owner_mismatch_page_ref_count > 0 {
        report
            .blockers
            .push("page refs disagree with expected object owners".to_string());
    }
    // Count live timestamped-kv pages (feature/sequence/ips and the context
    // timeline families). collect_live_page_entries already dedupes packed series
    // pages via unique_timestamped_kv_page_addresses, so this is the packed page
    // count. Previously this field was left at its default (0).
    const TIMESTAMPED_KINDS: [&str; 9] = [
        "feature",
        "sequence",
        "ips",
        "context_event",
        "context_index",
        "context_audit",
        "context_child",
        "context_summary",
        "context_compression",
    ];
    report.packed_timestamped_page_count = collect_live_page_entries(shard)
        .iter()
        .filter(|entry| TIMESTAMPED_KINDS.contains(&entry.kind.as_str()))
        .count() as u64;
    report.runtime_ready = report.blockers.is_empty();
    report
}

pub(super) fn slot_object_page_ownership_report(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> SlotObjectPageOwnershipReport {
    let mut report = SlotObjectPageOwnershipReport {
        shard_id,
        first_class_index_present: !shard.slot_index.slot_map.is_empty(),
        derived_from_model_maps: shard.slot_index.slot_map.is_empty(),
        ..SlotObjectPageOwnershipReport::default()
    };
    let entries = collect_live_page_entries(shard);
    report.page_ref_count = entries.len();
    for entry in entries {
        let routing_slot = entry.address.routing_slot.unwrap_or_default();
        if routing_slot < start_routing_slot || routing_slot > end_routing_slot {
            continue;
        }
        let expected_object_id = stable_page_object_id(
            shard_id,
            &entry.kind,
            &entry.object_key,
            entry.component.as_deref(),
        );
        let Some(slot) = shard.slot_index.slot_map.get(&routing_slot) else {
            report.missing_owner_page_ref_count =
                report.missing_owner_page_ref_count.saturating_add(1);
            continue;
        };
        if !slot.object_index.contains(&expected_object_id) {
            report.owner_mismatch_page_ref_count =
                report.owner_mismatch_page_ref_count.saturating_add(1);
        }
    }
    report
}

pub(super) fn merge_last_dump_sequence(
    mut summaries: Vec<SlotStorageSummary>,
    manifest: &SlotDumpManifest,
) -> Vec<SlotStorageSummary> {
    let dumped_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
    for summary in &mut summaries {
        if dumped_slots.contains(&summary.routing_slot) {
            summary.last_dump_sequence = manifest.index_log_sequence;
        }
    }
    summaries
}

pub(super) fn slot_dump_manifest_comparable_summaries(
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
) -> Vec<SlotStorageSummary> {
    comparable_slot_dump_summaries(
        slot_storage_summaries(shard, 0, u32::MAX)
            .into_iter()
            .filter(|summary| {
                selected_slots.is_empty() || selected_slots.contains(&summary.routing_slot)
            })
            .collect(),
    )
}

pub(super) fn comparable_slot_dump_summaries(
    mut summaries: Vec<SlotStorageSummary>,
) -> Vec<SlotStorageSummary> {
    for summary in &mut summaries {
        summary.dirty_object_count = 0;
        summary.dirty_generation = 0;
        summary.last_dump_sequence = 0;
        summary.page_segment_ids.sort_unstable();
        summary.page_segment_ids.dedup();
    }
    summaries.retain(|summary| {
        summary.object_count > 0
            || summary.page_ref_count > 0
            || summary.logical_bytes > 0
            || summary.physical_bytes > 0
    });
    summaries.sort_by_key(|summary| summary.routing_slot);
    summaries
}

pub(super) fn slot_dump_summary_matches_current_generation(
    manifest_summary: &SlotStorageSummary,
    current_summary: &SlotStorageSummary,
    manifest_slot_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
    current_slot_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
) -> bool {
    let mut manifest_segments = manifest_summary.page_segment_ids.clone();
    manifest_segments.sort_unstable();
    manifest_segments.dedup();
    let mut current_segments = current_summary.page_segment_ids.clone();
    current_segments.sort_unstable();
    current_segments.dedup();
    manifest_summary.routing_slot == current_summary.routing_slot
        && manifest_summary.dirty_generation == current_summary.dirty_generation
        && manifest_summary.object_count == current_summary.object_count
        && manifest_summary.page_ref_count == current_summary.page_ref_count
        && manifest_summary.logical_bytes == current_summary.logical_bytes
        && manifest_summary.physical_bytes == current_summary.physical_bytes
        && manifest_segments == current_segments
        && manifest_slot_fingerprints.get(&manifest_summary.routing_slot)
            == current_slot_fingerprints.get(&current_summary.routing_slot)
}

pub(super) fn slot_generation_fingerprints_by_slot(shard: &ShardState) -> BTreeMap<u32, BTreeSet<String>> {
    let mut by_slot = BTreeMap::<u32, BTreeSet<String>>::new();
    for entry in collect_live_page_entries(shard) {
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        by_slot.entry(routing_slot).or_default().insert(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            entry.kind,
            entry.object_key,
            entry.component.unwrap_or_default(),
            entry.address.page_segment_id,
            entry.address.offset,
            entry.address.length,
            entry.address.page_id.unwrap_or_default(),
            entry.address.object_id.unwrap_or_default(),
            entry.address.routing_slot.unwrap_or(routing_slot),
            entry.address.generation.unwrap_or_default(),
            entry.address.sha256.unwrap_or_default()
        ));
    }
    by_slot
}

pub(super) fn collect_live_page_addresses(shard: &ShardState) -> Vec<PageAddress> {
    collect_live_page_entries(shard)
        .into_iter()
        .map(|entry| entry.address)
        .collect()
}

pub(super) fn unique_timestamped_kv_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    let mut addresses = series
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    addresses.sort_by(|left, right| {
        left.page_segment_id
            .cmp(&right.page_segment_id)
            .then(left.offset.cmp(&right.offset))
            .then(left.length.cmp(&right.length))
    });
    addresses
}

pub(super) fn unique_feature_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    unique_timestamped_kv_page_addresses(series)
}

pub(super) fn timestamped_kv_series<'a>(
    shard: &'a ShardState,
) -> Vec<(&'static str, &'a str, &'a BTreeMap<u64, PageAddress>)> {
    let mut series = Vec::new();
    for (key, timeline) in &shard.features {
        series.push(("feature", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.sequences {
        series.push(("sequence", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.ips {
        series.push(("ips", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_events {
        series.push(("context_event", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_indexes {
        series.push(("context_index", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_audits {
        series.push(("context_audit", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_children {
        series.push(("context_child", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_summaries {
        series.push(("context_summary", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_compressions {
        series.push(("context_compression", key.as_str(), timeline));
    }
    series
}

pub(super) fn storage_feature_page_layout_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> StorageFeaturePageLayoutReport {
    let mut report = StorageFeaturePageLayoutReport::default();
    let mut family_reports = BTreeMap::<String, StorageTimestampedPageFamilyReport>::new();
    let mut inspected_addresses = HashSet::<PageAddress>::new();
    for (kind, key, series) in timestamped_kv_series(shard) {
        report.indexed_timestamped_points = report
            .indexed_timestamped_points
            .saturating_add(series.len());
        if kind == "feature" {
            report.indexed_feature_points =
                report.indexed_feature_points.saturating_add(series.len());
        }
        let family = family_reports.entry(kind.to_string()).or_insert_with(|| {
            StorageTimestampedPageFamilyReport {
                kind: kind.to_string(),
                ..StorageTimestampedPageFamilyReport::default()
            }
        });
        family.indexed_points = family.indexed_points.saturating_add(series.len());
        let mut timestamps_by_address = HashMap::<PageAddress, BTreeSet<u64>>::new();
        for (timestamp_ms, address) in series {
            timestamps_by_address
                .entry(address.clone())
                .or_default()
                .insert(*timestamp_ms);
        }
        report.unique_timestamped_page_refs = report
            .unique_timestamped_page_refs
            .saturating_add(timestamps_by_address.len());
        family.unique_page_refs = family
            .unique_page_refs
            .saturating_add(timestamps_by_address.len());
        if kind == "feature" {
            report.unique_feature_page_refs = report
                .unique_feature_page_refs
                .saturating_add(timestamps_by_address.len());
        }

        for (address, indexed_timestamps) in timestamps_by_address {
            inspected_addresses.insert(address.clone());
            match page_store.read(&address) {
                Ok(bytes) => match decode_feature_page_strict(&bytes) {
                    PackedFeaturePageDecode::Packed(points) => {
                        report.packed_timestamped_pages =
                            report.packed_timestamped_pages.saturating_add(1);
                        family.packed_pages = family.packed_pages.saturating_add(1);
                        if kind == "feature" {
                            report.packed_feature_pages =
                                report.packed_feature_pages.saturating_add(1);
                        }
                        let mut packed_timestamp_counts = BTreeMap::<u64, usize>::new();
                        for point in &points {
                            let count = packed_timestamp_counts
                                .entry(point.timestamp_ms)
                                .or_default();
                            if *count == 1 {
                                report.duplicate_packed_timestamps.push(
                                    feature_page_timestamp_mismatch(
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
                                feature_page_timestamp_mismatch(kind, key, timestamp_ms, &address),
                            );
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                        for timestamp_ms in
                            packed_timestamps.difference(&indexed_timestamps).copied()
                        {
                            report
                                .orphan_packed_timestamps
                                .push(feature_page_timestamp_mismatch(
                                    kind,
                                    key,
                                    timestamp_ms,
                                    &address,
                                ));
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                    }
                    PackedFeaturePageDecode::Corrupt(error) => {
                        report
                            .corrupt_packed_feature_pages
                            .push(feature_page_error(kind, key, &address, error));
                        family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                    }
                    PackedFeaturePageDecode::Legacy => {
                        report.legacy_timestamped_value_pages =
                            report.legacy_timestamped_value_pages.saturating_add(1);
                        family.legacy_value_pages = family.legacy_value_pages.saturating_add(1);
                        if kind == "feature" {
                            report.legacy_feature_value_pages =
                                report.legacy_feature_value_pages.saturating_add(1);
                        }
                        if indexed_timestamps.len() > 1 {
                            report.corrupt_packed_feature_pages.push(feature_page_error(
                                kind,
                                key,
                                &address,
                                "legacy timestamped value page shared by multiple timestamps",
                            ));
                            family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                        }
                    }
                },
                Err(err) => {
                    report.corrupt_packed_feature_pages.push(feature_page_error(
                        kind,
                        key,
                        &address,
                        err.to_string(),
                    ));
                    family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                }
            }
        }
    }
    for entry in collect_slot_index_live_page_entries(shard) {
        if entry.deleted || inspected_addresses.contains(&entry.address) {
            continue;
        }
        if !matches!(
            entry.kind.as_str(),
            "feature"
                | "sequence"
                | "ips"
                | "context_event"
                | "context_index"
                | "context_audit"
                | "context_child"
                | "context_summary"
                | "context_compression"
        ) {
            continue;
        }
        let family = family_reports.entry(entry.kind.clone()).or_insert_with(|| {
            StorageTimestampedPageFamilyReport {
                kind: entry.kind.clone(),
                ..StorageTimestampedPageFamilyReport::default()
            }
        });
        report.unique_timestamped_page_refs = report.unique_timestamped_page_refs.saturating_add(1);
        family.unique_page_refs = family.unique_page_refs.saturating_add(1);
        if entry.kind == "feature" {
            report.unique_feature_page_refs = report.unique_feature_page_refs.saturating_add(1);
        }
        match page_store.read(&entry.address) {
            Ok(bytes) => match decode_feature_page_strict(&bytes) {
                PackedFeaturePageDecode::Packed(points) => {
                    report.packed_timestamped_pages =
                        report.packed_timestamped_pages.saturating_add(1);
                    family.packed_pages = family.packed_pages.saturating_add(1);
                    if entry.kind == "feature" {
                        report.packed_feature_pages = report.packed_feature_pages.saturating_add(1);
                    }
                    for point in points {
                        report
                            .orphan_packed_timestamps
                            .push(feature_page_timestamp_mismatch(
                                &entry.kind,
                                &entry.object_key,
                                point.timestamp_ms,
                                &entry.address,
                            ));
                        family.mismatch_count = family.mismatch_count.saturating_add(1);
                    }
                }
                PackedFeaturePageDecode::Corrupt(error) => {
                    report.corrupt_packed_feature_pages.push(feature_page_error(
                        &entry.kind,
                        &entry.object_key,
                        &entry.address,
                        error,
                    ));
                    family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                }
                PackedFeaturePageDecode::Legacy => {
                    report.legacy_timestamped_value_pages =
                        report.legacy_timestamped_value_pages.saturating_add(1);
                    family.legacy_value_pages = family.legacy_value_pages.saturating_add(1);
                    if entry.kind == "feature" {
                        report.legacy_feature_value_pages =
                            report.legacy_feature_value_pages.saturating_add(1);
                    }
                }
            },
            Err(err) => {
                report.corrupt_packed_feature_pages.push(feature_page_error(
                    &entry.kind,
                    &entry.object_key,
                    &entry.address,
                    err.to_string(),
                ));
                family.corrupt_pages = family.corrupt_pages.saturating_add(1);
            }
        }
    }
    report.families = family_reports.into_values().collect();
    report
}

pub(super) fn feature_page_error(
    kind: &str,
    key: &str,
    address: &PageAddress,
    error: impl Into<String>,
) -> StorageFeaturePageError {
    StorageFeaturePageError {
        kind: kind.to_string(),
        key: key.to_string(),
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
        error: error.into(),
    }
}

pub(super) fn feature_page_timestamp_mismatch(
    kind: &str,
    key: &str,
    timestamp_ms: u64,
    address: &PageAddress,
) -> StorageFeaturePageTimestampMismatch {
    StorageFeaturePageTimestampMismatch {
        kind: kind.to_string(),
        key: key.to_string(),
        timestamp_ms,
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
    }
}

