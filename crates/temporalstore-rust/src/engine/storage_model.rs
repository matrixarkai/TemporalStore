pub(super) fn storage_model_code(kind: &str) -> u8 {
    match kind {
        "string" => 1,
        "hash" => 2,
        "set" => 3,
        "feature" => 4,
        "sequence" => 5,
        "ips" => 6,
        "control_state" => 7,
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

pub(super) fn compaction_layout_policy_for_model(model_id: &str) -> &'static str {
    match model_id {
        "string" | "control_state" | "context_node" | "context_entity" | "context_embedding" => {
            "single_page_object"
        }
        "hash" | "set" => "component_page_object",
        "feature" | "sequence" | "ips" => "timestamped_chunked_pages",
        model if model.starts_with("context_") => "context_timeline_or_sidecar_pages",
        _ => "generic_page_object",
    }
}

pub(super) fn compaction_object_page_packing_enabled(model_id: &str) -> bool {
    matches!(
        compaction_layout_policy_for_model(model_id),
        "single_page_object" | "component_page_object"
    )
}
