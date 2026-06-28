use serde::{Deserialize, Serialize};

use super::state::ShardState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectManagerRuntimeReport {
    pub object_manager_runtime_module: bool,
    pub slot_index_authority: bool,
    pub live_object_count: usize,
    pub live_page_ref_count: usize,
    pub missing_object_owner_refs: usize,
    pub reused_object_ids: usize,
}

pub(super) fn runtime_report(shard: &ShardState) -> ObjectManagerRuntimeReport {
    let mut object_ids = std::collections::BTreeSet::new();
    let mut missing_object_owner_refs = 0usize;
    let mut object_ref_counts = std::collections::BTreeMap::<u64, usize>::new();
    let mut live_page_ref_count = 0usize;

    for slot in shard.slot_index.slots.values() {
        for page in slot.page_refs.values() {
            live_page_ref_count = live_page_ref_count.saturating_add(1);
            if page.deleted {
                continue;
            }
            object_ids.insert(page.object_id);
            *object_ref_counts.entry(page.object_id).or_default() += 1;
            if page.address.object_id != Some(page.object_id) {
                missing_object_owner_refs = missing_object_owner_refs.saturating_add(1);
            }
        }
    }

    ObjectManagerRuntimeReport {
        object_manager_runtime_module: true,
        slot_index_authority: !shard.slot_index.slots.is_empty(),
        live_object_count: object_ids.len(),
        live_page_ref_count,
        missing_object_owner_refs,
        reused_object_ids: object_ref_counts.values().filter(|refs| **refs > 1).count(),
    }
}
