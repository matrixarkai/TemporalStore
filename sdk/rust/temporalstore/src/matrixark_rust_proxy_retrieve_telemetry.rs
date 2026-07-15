use serde_json::{json, Value};

pub(crate) struct RetrieveDropCounts {
    pub(crate) over_budget: u64,
    pub(crate) cross_budget: u64,
    pub(crate) cross_session_cap: u64,
    pub(crate) cross_candidate_cap: u64,
    pub(crate) policy_ref: u64,
    pub(crate) duplicate_ref: u64,
    pub(crate) scan_dropped: u64,
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

pub(crate) fn total_dropped_ref_count(counts: RetrieveDropCounts) -> u64 {
    counts.over_budget
        + counts.cross_budget
        + counts.cross_session_cap
        + counts.cross_candidate_cap
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
