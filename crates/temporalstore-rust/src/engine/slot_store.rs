use serde::{Deserialize, Serialize};

use super::state::{ShardState, SlotLayoutState};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SlotRuntimeState {
    pub routing_slot: u32,
    pub layout: String,
    pub object_ids: Vec<u64>,
    pub page_ref_count: usize,
    pub dirty: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub dirty_generation: u64,
    pub last_dump_sequence: u64,
    pub deleted_page_ref_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SlotStoreRuntimeReport {
    pub slot_store_runtime_module: bool,
    pub slot_index_authority: bool,
    pub slot_count: usize,
    pub page_ref_count: usize,
    pub dirty_slot_count: usize,
    pub empty_slots: usize,
    pub single_object_slots: usize,
    pub single_page_object_slots: usize,
    pub multi_page_object_slots: usize,
    pub multi_object_slots: usize,
    pub deleted_page_ref_count: usize,
    pub loading_slot_count: usize,
    pub in_memory_slot_count: usize,
    pub ttl_slot_count: usize,
    pub max_dirty_generation: u64,
    pub slots: Vec<SlotRuntimeState>,
}

pub(super) fn runtime_report(shard: &ShardState) -> SlotStoreRuntimeReport {
    let mut report = SlotStoreRuntimeReport {
        slot_store_runtime_module: true,
        slot_index_authority: !shard.slot_index.slots.is_empty(),
        slot_count: shard.slot_index.slots.len(),
        page_ref_count: 0,
        dirty_slot_count: 0,
        empty_slots: 0,
        single_object_slots: 0,
        single_page_object_slots: 0,
        multi_page_object_slots: 0,
        multi_object_slots: 0,
        deleted_page_ref_count: 0,
        loading_slot_count: 0,
        in_memory_slot_count: 0,
        ttl_slot_count: 0,
        max_dirty_generation: 0,
        slots: Vec::new(),
    };

    for slot in shard.slot_index.slots.values() {
        report.page_ref_count = report.page_ref_count.saturating_add(slot.page_refs.len());
        if slot.dirty {
            report.dirty_slot_count = report.dirty_slot_count.saturating_add(1);
        }
        if slot.loading {
            report.loading_slot_count = report.loading_slot_count.saturating_add(1);
        }
        if slot.in_memory {
            report.in_memory_slot_count = report.in_memory_slot_count.saturating_add(1);
        }
        if slot.ttl_ms.is_some() {
            report.ttl_slot_count = report.ttl_slot_count.saturating_add(1);
        }
        report.max_dirty_generation = report.max_dirty_generation.max(slot.dirty_generation);
        let deleted_page_ref_count = slot.page_refs.values().filter(|page| page.deleted).count();
        report.deleted_page_ref_count = report
            .deleted_page_ref_count
            .saturating_add(deleted_page_ref_count);
        match slot.layout {
            SlotLayoutState::Empty => report.empty_slots = report.empty_slots.saturating_add(1),
            SlotLayoutState::SingleObject => {
                report.single_object_slots = report.single_object_slots.saturating_add(1)
            }
            SlotLayoutState::SinglePageObject => {
                report.single_page_object_slots = report.single_page_object_slots.saturating_add(1)
            }
            SlotLayoutState::MultiPageObject => {
                report.multi_page_object_slots = report.multi_page_object_slots.saturating_add(1)
            }
            SlotLayoutState::MultiObject => {
                report.multi_object_slots = report.multi_object_slots.saturating_add(1)
            }
        }
        report.slots.push(SlotRuntimeState {
            routing_slot: slot.routing_slot,
            layout: slot_layout_name(slot.layout).to_string(),
            object_ids: slot.object_ids.iter().copied().collect(),
            page_ref_count: slot.page_refs.len(),
            dirty: slot.dirty,
            meta_loaded: slot.meta_loaded,
            loading: slot.loading,
            in_memory: slot.in_memory,
            ttl_ms: slot.ttl_ms,
            dirty_generation: slot.dirty_generation,
            last_dump_sequence: slot.last_dump_sequence,
            deleted_page_ref_count,
        });
    }

    report
}

fn slot_layout_name(layout: SlotLayoutState) -> &'static str {
    match layout {
        SlotLayoutState::Empty => "empty",
        SlotLayoutState::SingleObject => "single_object",
        SlotLayoutState::SinglePageObject => "single_page_object",
        SlotLayoutState::MultiPageObject => "multi_page_object",
        SlotLayoutState::MultiObject => "multi_object",
    }
}
