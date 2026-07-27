use serde_json::{json, Value};

pub(crate) struct RetrieveDropCounts {
    pub(crate) over_budget: u64,
    pub(crate) cross_budget: u64,
    pub(crate) cross_session_cap: u64,
    pub(crate) cross_candidate_cap: u64,
    pub(crate) entity_bridge_slot_reserved: u64,
    pub(crate) policy_ref: u64,
    pub(crate) duplicate_ref: u64,
    pub(crate) scan_dropped: u64,
}

pub(crate) struct RetrieveDroppedRefs {
    pub(crate) over_budget: u64,
    pub(crate) cross_budget: u64,
    pub(crate) cross_session_cap: u64,
    pub(crate) cross_candidate_cap: u64,
    pub(crate) entity_bridge_slot_reserved: u64,
    pub(crate) low_score: u64,
    pub(crate) duplicate_ref: u64,
    pub(crate) policy_ref: u64,
}

pub(crate) fn mark_native_pack_scan_stats(mut scan_stats: Value) -> Value {
    if let Some(stats) = scan_stats.as_object_mut() {
        stats.insert("native_pack_assembly".to_string(), json!(true));
        stats.insert(
            "pack_assembly_location".to_string(),
            json!("rust_proxy_native"),
        );
        stats.insert("next_native_gap".to_string(), json!(""));
    }
    scan_stats
}

pub(crate) fn dropped_refs_json(dropped: RetrieveDroppedRefs) -> Value {
    json!({
        "over_budget": dropped.over_budget,
        "cross_session_budget": dropped.cross_budget,
        "cross_session_session_cap": dropped.cross_session_cap,
        "cross_session_candidate_cap": dropped.cross_candidate_cap,
        "entity_bridge_slot_reserved": dropped.entity_bridge_slot_reserved,
        "low_score": dropped.low_score,
        "duplicate_ref": dropped.duplicate_ref,
        "policy_ref": dropped.policy_ref,
        "reason_counts": {
            "over_budget": dropped.over_budget,
            "cross_session_budget": dropped.cross_budget,
            "cross_session_session_cap": dropped.cross_session_cap,
            "cross_session_candidate_cap": dropped.cross_candidate_cap,
            "entity_bridge_slot_reserved": dropped.entity_bridge_slot_reserved,
            "low_score": dropped.low_score,
            "duplicate_ref": dropped.duplicate_ref,
            "policy_ref": dropped.policy_ref
        }
    })
}

pub(crate) fn total_dropped_ref_count(counts: RetrieveDropCounts) -> u64 {
    counts.over_budget
        + counts.cross_budget
        + counts.cross_session_cap
        + counts.cross_candidate_cap
        + counts.entity_bridge_slot_reserved
        + counts.policy_ref
        + counts.duplicate_ref
        + counts.scan_dropped
}

pub(crate) fn same_session_selected_ref_count(selected: &[Value]) -> usize {
    selected
        .iter()
        .filter(|item| item.get("session_continuity").and_then(Value::as_str) == Some("same_session"))
        .count()
}
