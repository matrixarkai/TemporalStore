use serde::{Deserialize, Serialize};

use super::state::{ShardState, SlotLayoutState};

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
    };

    for slot in shard.slot_index.slots.values() {
        report.page_ref_count = report.page_ref_count.saturating_add(slot.page_refs.len());
        if slot.dirty {
            report.dirty_slot_count = report.dirty_slot_count.saturating_add(1);
        }
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
    }

    report
}
